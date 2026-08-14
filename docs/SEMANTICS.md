# Runtime semantics

## Source identity

For `(year, month)`, the DAG expects exactly:

```text
landing/fhvhv_tripdata_YYYY-MM.parquet
```

The upstream producer owns landing. Airflow verifies the S3 object is non-empty
and has a lowercase 64-character SHA-256 metadata value. Bronze repeats those
checks before reading.

The locked identity is:

```text
source URI + SHA-256 + byte size + year + month
```

The same identity produces the same `ingestion_run_id`. If any identity field
changes after the month exists in the operational manifest, Bronze fails
before mutating canonical data.

## Row identity

`row_id` hashes the policy version and every ordered canonical source field.
It is the only exact deduplication and dbt merge key.

`business_trip_key` hashes fewer trip descriptors for investigation. It may
group probable duplicates but cannot delete or quarantine a row by itself.

## Retry and rerun

- Airflow task retry reuses the same source and run ID.
- A failure before an Iceberg commit has no published layer evidence.
- A failure after a Bronze commit preserves its count and snapshot.
- Silver retry may reclassify and replace only the requested month.
- Once `silver_published`, identical reruns skip open-layer rewrites.
- dbt executes again and validates Gold.
- The first successful dbt artifact for the stable source is retained.
- Reconciliation and bounded verification run again.
- The same logical publication is reused; attempt timestamps/query IDs are not
  release identity.

There is no `force` parameter.

## Classification

Silver applies one ordered reason policy from
`etl/contracts/nyc_hvfhs_quality.py`. A row receives exactly one first reason
or enters Silver. Exact duplicate `row_id` has the final priority so a more
fundamental row defect remains visible.

The mandatory gates are:

```text
Bronze = Silver + quarantine
Silver = Gold fct_trips
```

## Reference data

Taxi Zones is loaded once into an unpartitioned Bronze reference table. IDs
must be non-null and unique. The identical checksum is reused. Changed content
fails with an explicit migration requirement; it is not silently rewritten
for every month.

## Publication

Publication is allowed only with:

- successful dbt `run_results.json`;
- all three open-layer snapshot IDs;
- both reconciliation invariants;
- the exact six Gold relation names.

Publication contains no credentials and no Gold Iceberg path because Gold is
managed by Redshift.

## Schema evolution

The only allowed post-baseline change is nullable `cbd_congestion_fee` in 2025
Bronze, Silver, and quarantine. Apply the explicit evolution script, ingest a
2025 month, then run `verify_nyc_snapshot.py` on transient EMR against the
retained 2024 snapshot. Compaction, snapshot expiration, orphan cleanup, and
partition evolution are out of scope.
