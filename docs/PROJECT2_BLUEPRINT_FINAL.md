# Final blueprint: production-shaped, cost-bounded lakehouse

## Objective

Prove the architecture and correctness semantics of an industrial monthly data
product with a small deployment. The project verifies control-plane,
cross-engine, identity, rerun, and publication behavior. It does not claim a
TB/PB performance benchmark.

```text
Upstream producer
  -> immutable S3 landing
  -> regular MWAA (Airflow 3)
  -> transient EMR on EC2 / PySpark Bronze
  -> transient EMR on EC2 / PySpark Silver + quarantine
  -> S3 Iceberg + Glue Data Catalog
  -> Redshift Serverless external schemas
  -> dbt-redshift managed Gold
  -> reconciliation
  -> deterministic publication
  -> bounded read-after-publish verification
```

## Architectural ownership

| Boundary | Owner | Responsibility |
| --- | --- | --- |
| Input | Upstream producer + S3 | Land immutable objects and SHA-256 metadata |
| Control plane | regular MWAA | Ordering, retry, rerun, backfill, task evidence |
| Batch compute | transient EMR on EC2 | Bronze and Silver/quarantine Spark work; Primary On-Demand and bounded Core Spot |
| Open storage | S3 + Iceberg | ACID Bronze, Silver, quarantine, operational state |
| Shared metadata | Glue Data Catalog | Metadata only; one catalog shared by EMR and Spectrum |
| Gold transform | Cosmos Watcher + dbt-redshift | One build, model/test visibility, six managed Redshift relations |
| Serving | Redshift Serverless | SQL contract for analytics consumers |
| Release | S3 publication JSON | Durable source/count/snapshot/dbt evidence |

There is one orchestrator, one state table, one dbt execution path, and one
publication path.

## Input boundary

The codebase does not download or upload NYC TLC data. Before a DAG run, the
producer must provide:

```text
s3://<bucket>/landing/fhvhv_tripdata_YYYY-MM.parquet
s3://<bucket>/reference/taxi_zone_lookup.csv
```

Each object is non-empty and has lowercase SHA-256 in S3 user metadata key
`sha256`. Airflow uses `HeadObject` to bind the request to URI, checksum, and
size before it submits Spark. This is the project input contract, not an
ingestion subsystem.

## Identity and layers

One month is identified by URI + SHA-256 + size + year + month. Its stable run
ID is derived from that identity.

- `row_id` is the SHA-256 of the ordered, versioned exact-row representation.
- `business_trip_key` is a probable-trip analytical key only.
- `identity_policy_version` makes the ordered field set explicit.
- The sole policy implementation is
  `etl/contracts/nyc_hvfhs_identity.py`.

Bronze preserves source fields and adds ingestion/identity metadata. It checks
the object, schema, and non-empty input, creates the locked base Iceberg tables
when absent, and replaces only the requested month.

Taxi Zones is one immutable reference table, loaded once. A changed reference
requires an explicit migration.

Silver owns timestamp, zone, numeric, exact-deduplication, and reason-priority
rules. Every Bronze row is written exactly once to Silver or quarantine:

```text
bronze_count = silver_count + quarantine_count
```

Gold remains exactly:

```text
dim_date
dim_operator
dim_zone
fct_trips
mart_hourly_zone_demand
mart_operator_metrics
```

`fct_trips` merges on `row_id`. The cross-engine release gate also requires:

```text
silver_count = gold_fct_trips_count
```

## Orchestration and rerun

The monthly DAG is:

```text
prepare_month
-> create_emr_cluster
-> bronze_ingestion_emr -> bronze_ingestion_complete
-> silver_transform_emr -> silver_transform_complete
-> terminate_emr_cluster
-> dbt_build
-> dbt_result_artifact
-> reconciliation
-> publication_manifest
-> verification
```

`dbt_build` is a Cosmos `DbtTaskGroup` in Watcher mode. Cosmos runs one
`dbt build` producer, projects model/test status into Airflow, and archives the
complete successful `run_results.json` before its temporary project is removed.
dbt remains the owner of model dependencies and SQL execution. A bounded MWAA
startup script installs the pinned dbt adapter in a separate virtualenv because
Airflow 3.2.1 and dbt have incompatible transitive constraints.

The completed identical source reuses existing Bronze/Silver/quarantine
snapshots. dbt safely rebuilds/merges Gold, reconciliation runs again, and the
same logical publication is reused. Changed source identity is rejected. There
is no second manifest state machine and no `force` switch.

The companion backfill DAG triggers exactly four monthly DAG runs in order. It
is intentionally not a generic framework.

## Publication and verification

Reconciliation uses one Redshift Data API statement to read three open-layer
counts and operational snapshot IDs through Spectrum plus the managed Gold
fact count. Publication uses that evidence directly; it does not query
"latest snapshot" again.

The publication JSON records source identity, policy, three table/snapshot/count
triples, six Gold relation names, the dbt artifact URI/SHA-256, and
reconciliation evidence. The key is stable for the immutable source. Attempt
timestamps and query IDs do not turn an identical rerun into a conflicting
release.

Verification is intentionally small:

1. read publication and verify its SHA-256 and identity;
2. use one Redshift statement to read a month-scoped Silver external count and
   Gold managed count;
3. compare both with the published counts.

## Deployment boundary

Terraform defines one private regular MWAA environment, the EMR service and
EC2 runtime roles for transient clusters, one Redshift Serverless
namespace/workgroup, one private S3 bucket, three Glue Data Catalog namespaces,
and IAM boundaries. Each cluster is created by the DAG, not Terraform.

Regular MWAA is provisioned and does not scale to zero. `mw1.small`, two maximum
workers, and the separately approved teardown plan bound the baseline cost.
Terraform requires an existing VPC and two private subnets with the AWS/PyPI
egress needed by MWAA.

## Deliberate exclusions

No producer uploader, Lambda/EventBridge trigger, EC2 Airflow runner, MWAA
Serverless, EKS, Glue ETL job, second query engine, Lake Formation,
dashboard, streaming system, lineage platform, generic dataset framework, or
Iceberg maintenance suite is part of the baseline.

The sole post-baseline semantic is adding nullable `cbd_congestion_fee` for
2025, ingesting a new snapshot, and querying the retained 2024 snapshot with
one bounded transient EMR Spark verification job.

`CODEBASE-READY` requires all credential-independent checks to pass.
`DEPLOYMENT-VERIFIED` remains **NOT VERIFIED** until a real bounded AWS run
retains the required evidence.
