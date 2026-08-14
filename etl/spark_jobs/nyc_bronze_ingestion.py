"""Retry-safe transient EMR Bronze ingestion for one immutable NYC month."""

from __future__ import annotations

from urllib.parse import urlparse

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    current_timestamp,
    input_file_name,
    lit,
    sum as spark_sum,
    when,
)
from pyspark.storagelevel import StorageLevel

from etl.contracts.nyc_hvfhs_identity import (
    identity_policy_version,
    spark_identity_expressions,
)
from etl.iceberg.catalog import TABLE_SPECS, namespace_ddl, table_ddl
from etl.sources.nyc_hvfhs import (
    SourceFile,
    stable_run_id,
    validate_landed_source,
    validate_trip_schema,
)
from etl.spark_jobs.arguments import parse_arguments


args = parse_arguments(
    [
        "SOURCE_URI",
        "SOURCE_YEAR",
        "SOURCE_MONTH",
        "SOURCE_CHECKSUM",
        "SOURCE_SIZE_BYTES",
        "INGESTION_RUN_ID",
        "TAXI_ZONE_URI",
        "TAXI_ZONE_CHECKSUM",
    ],
    {
        "CATALOG_NAME": "glue_catalog",
        "BRONZE_DATABASE": "bronze",
        "OPS_DATABASE": "ops",
    },
)


def _optional_arg(name: str, default: str) -> str:
    return args.get(name, default)


def _table(namespace: str, name: str) -> str:
    database = _optional_arg(f"{namespace.upper()}_DATABASE", namespace)
    return f"{_optional_arg('CATALOG_NAME', 'glue_catalog')}.{database}.{name}"


TRIPS_TABLE = _table("bronze", "bronze_hvfhs_trips")
ZONES_TABLE = _table("bronze", "bronze_taxi_zones")
MANIFEST_TABLE = _table("ops", "source_run_manifest")


def _ensure_base_tables(spark) -> None:
    """Create the locked Iceberg contracts during the first real Bronze run."""

    catalog = _optional_arg("CATALOG_NAME", "glue_catalog")
    namespace_map = {
        namespace: _optional_arg(f"{namespace.upper()}_DATABASE", namespace)
        for namespace in {spec.namespace for spec in TABLE_SPECS}
    }
    warehouse_uri = spark.conf.get(f"spark.sql.catalog.{catalog}.warehouse")
    for namespace in sorted(namespace_map.values()):
        spark.sql(namespace_ddl(namespace, catalog=catalog))
    for spec in TABLE_SPECS:
        mapped = type(spec)(
            namespace_map[spec.namespace], spec.name, spec.columns, spec.partitioned_by
        )
        spark.sql(table_ddl(mapped, warehouse_uri, catalog=catalog))


def _verify_landed_object(
    uri: str, expected_checksum: str, *, expected_size: int | None = None
) -> None:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("Landed objects must use a complete s3://bucket/key URI.")
    response = boto3.client("s3").head_object(Bucket=parsed.netloc, Key=key)
    if response.get("Metadata", {}).get("sha256") != expected_checksum:
        raise ValueError(f"Landed object SHA-256 metadata does not match {uri}.")
    if (
        expected_size is not None
        and int(response.get("ContentLength", -1)) != expected_size
    ):
        raise ValueError(f"Landed object byte size does not match {uri}.")


def _guard_month_identity(spark) -> bool:
    existing = spark.sql(
        f"SELECT source_uri, source_checksum, source_size_bytes, run_status "
        f"FROM {MANIFEST_TABLE} "
        f"WHERE source_year={int(args['SOURCE_YEAR'])} "
        f"AND source_month={int(args['SOURCE_MONTH'])} LIMIT 1"
    ).collect()
    if not existing:
        return False
    row = existing[0]
    if (
        row.source_uri != args["SOURCE_URI"]
        or row.source_checksum != args["SOURCE_CHECKSUM"]
        or int(row.source_size_bytes) != int(args["SOURCE_SIZE_BYTES"])
    ):
        raise ValueError(
            "Changed source URI, checksum, or byte size is blocked for this month."
        )
    return row.run_status == "silver_published"


def _record_manifest(
    spark,
    *,
    status: str,
    bronze_count: int = 0,
    bronze_snapshot_id: str | None = None,
    failure_stage: str | None = None,
    failure_message: str | None = None,
) -> None:
    update = spark.createDataFrame(
        [
            (
                args["SOURCE_URI"],
                args["SOURCE_CHECKSUM"],
                int(args["SOURCE_SIZE_BYTES"]),
                int(args["SOURCE_YEAR"]),
                int(args["SOURCE_MONTH"]),
                args["INGESTION_RUN_ID"],
                identity_policy_version(int(args["SOURCE_YEAR"])),
                status,
                bronze_count,
                bronze_snapshot_id,
                failure_stage,
                failure_message,
            )
        ],
        "source_uri string, source_checksum string, source_size_bytes long, "
        "source_year int, source_month int, ingestion_run_id string, "
        "identity_policy_version string, run_status string, bronze_row_count long, "
        "bronze_snapshot_id string, failure_stage string, failure_message string",
    )
    update.createOrReplaceTempView("manifest_update")
    spark.sql(
        f"""
        MERGE INTO {MANIFEST_TABLE} target
        USING manifest_update source
          ON target.source_year = source.source_year
         AND target.source_month = source.source_month
        WHEN MATCHED THEN UPDATE SET
          ingestion_run_id = source.ingestion_run_id,
          identity_policy_version = source.identity_policy_version,
          run_status = source.run_status,
          bronze_row_count = CASE WHEN source.run_status='failed' THEN target.bronze_row_count ELSE source.bronze_row_count END,
          bronze_snapshot_id = CASE WHEN source.run_status='failed' THEN target.bronze_snapshot_id ELSE source.bronze_snapshot_id END,
          silver_row_count = CASE WHEN source.run_status='bronze_published' THEN 0 ELSE target.silver_row_count END,
          quarantine_row_count = CASE WHEN source.run_status='bronze_published' THEN 0 ELSE target.quarantine_row_count END,
          silver_snapshot_id = CASE WHEN source.run_status='bronze_published' THEN NULL ELSE target.silver_snapshot_id END,
          quarantine_snapshot_id = CASE WHEN source.run_status='bronze_published' THEN NULL ELSE target.quarantine_snapshot_id END,
          completed_at = CASE WHEN source.run_status='bronze_published' THEN NULL ELSE target.completed_at END,
          failure_stage = source.failure_stage,
          failure_message = source.failure_message,
          updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
          source_uri, source_checksum, source_size_bytes, source_year, source_month,
          ingestion_run_id, identity_policy_version, run_status, first_seen_at,
          updated_at, bronze_row_count, silver_row_count, quarantine_row_count,
          bronze_snapshot_id, failure_stage, failure_message
        ) VALUES (
          source.source_uri, source.source_checksum, source.source_size_bytes,
          source.source_year, source.source_month, source.ingestion_run_id,
          source.identity_policy_version, source.run_status, current_timestamp(),
          current_timestamp(), source.bronze_row_count, 0, 0,
          source.bronze_snapshot_id, source.failure_stage, source.failure_message
        )
        """
    )


def _ensure_reference_zones(spark) -> None:
    existing = [
        row._source_checksum
        for row in spark.table(ZONES_TABLE)
        .select("_source_checksum")
        .distinct()
        .limit(2)
        .collect()
    ]
    if existing:
        if existing != [args["TAXI_ZONE_CHECKSUM"]]:
            raise ValueError(
                "Taxi Zone reference content changed; use an explicit reference migration."
            )
        return
    zones = (
        spark.read.option("header", True)
        .csv(args["TAXI_ZONE_URI"])
        .select(
            col("LocationID").cast("int").alias("LocationID"),
            "Borough",
            "Zone",
            "service_zone",
        )
        .withColumn("_source_uri", lit(args["TAXI_ZONE_URI"]))
        .withColumn("_source_checksum", lit(args["TAXI_ZONE_CHECKSUM"]))
        .withColumn("_ingested_at", current_timestamp())
    )
    profile = zones.agg(
        count(lit(1)).alias("row_count"),
        countDistinct("LocationID").alias("distinct_ids"),
        spark_sum(when(col("LocationID").isNull(), 1).otherwise(0)).alias("null_ids"),
    ).first()
    if (
        int(profile.row_count) <= 0
        or int(profile.null_ids or 0) > 0
        or int(profile.distinct_ids) != int(profile.row_count)
    ):
        raise ValueError(
            "Taxi Zone reference requires unique, non-null LocationID rows."
        )
    zones.writeTo(ZONES_TABLE).overwritePartitions()


def main() -> None:
    source = SourceFile(
        int(args["SOURCE_YEAR"]),
        int(args["SOURCE_MONTH"]),
        args["SOURCE_URI"],
        args["SOURCE_CHECKSUM"],
        int(args["SOURCE_SIZE_BYTES"]),
    )
    validate_landed_source(source)
    if stable_run_id(source) != args["INGESTION_RUN_ID"]:
        raise ValueError("INGESTION_RUN_ID does not match immutable source identity.")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _ensure_base_tables(spark)
    trips = None
    identity_accepted = False
    try:
        _verify_landed_object(
            args["SOURCE_URI"],
            args["SOURCE_CHECKSUM"],
            expected_size=int(args["SOURCE_SIZE_BYTES"]),
        )
        _verify_landed_object(args["TAXI_ZONE_URI"], args["TAXI_ZONE_CHECKSUM"])
        _ensure_reference_zones(spark)
        already_complete = _guard_month_identity(spark)
        identity_accepted = True
        if already_complete:
            return

        source_frame = spark.read.parquet(args["SOURCE_URI"])
        validate_trip_schema(source_frame.columns, int(args["SOURCE_YEAR"]))
        exact_id, probable_key, policy = spark_identity_expressions(
            int(args["SOURCE_YEAR"])
        )
        trips = (
            source_frame.withColumn("_source_uri", lit(args["SOURCE_URI"]))
            .withColumn("_source_file", input_file_name())
            .withColumn("_source_year", lit(int(args["SOURCE_YEAR"])))
            .withColumn("_source_month", lit(int(args["SOURCE_MONTH"])))
            .withColumn("_source_checksum", lit(args["SOURCE_CHECKSUM"]))
            .withColumn("_ingestion_run_id", lit(args["INGESTION_RUN_ID"]))
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("row_id", exact_id)
            .withColumn("business_trip_key", probable_key)
            .withColumn("identity_policy_version", policy)
        )
        trips.persist(StorageLevel.MEMORY_AND_DISK)
        bronze_count = trips.count()
        if bronze_count <= 0:
            raise ValueError("Bronze input must be non-empty.")
        trips.writeTo(TRIPS_TABLE).overwritePartitions()
        snapshot = spark.sql(
            f"SELECT snapshot_id FROM {TRIPS_TABLE}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).first()
        if snapshot is None:
            raise ValueError("Bronze commit did not expose an Iceberg snapshot.")
        _record_manifest(
            spark,
            status="bronze_published",
            bronze_count=bronze_count,
            bronze_snapshot_id=str(snapshot.snapshot_id),
        )
    except Exception as error:
        if identity_accepted:
            _record_manifest(
                spark,
                status="failed",
                failure_stage="bronze",
                failure_message=str(error)[:2000],
            )
        raise
    finally:
        if trips is not None:
            trips.unpersist()


if __name__ == "__main__":
    main()
