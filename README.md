# NYC HVFHV AWS lakehouse

This repository is a production-shaped, cost-bounded AWS data engineering
project for monthly NYC ride-hailing data. It turns one immutable source month
into governed Bronze and Silver Iceberg datasets and a tested Gold analytical
product in Redshift.

The deployment is intentionally small to control cost. The architecture keeps
the same separation of orchestration, compute, storage, metadata, modeling,
and serving that a larger production platform would use.

## Architecture

```text
upstream S3 landing
        -> MWAA / Airflow 3
        -> transient EMR on EC2 (On-Demand Primary + Spot Core)
        -> S3 Iceberg + Glue Data Catalog
        -> Redshift Serverless / Spectrum
        -> Cosmos + dbt managed Gold
        -> reconcile -> publish -> verify
```

The main responsibilities are deliberately explicit:

| Component | Responsibility |
| --- | --- |
| S3 landing | Holds immutable monthly source files supplied by the upstream producer. |
| Amazon MWAA / Airflow | Coordinates the monthly workflow, dependencies, retries, and reruns. |
| Transient EMR on EC2 + Spark | Creates one short-lived cluster per monthly run; it reads the source, creates Bronze, and produces validated Silver and quarantine records. The primary node is On-Demand and the single Core worker uses Spot with provisioning fallback. |
| S3 + Apache Iceberg | Stores the canonical open Bronze, Silver, quarantine, and operational tables with transactional snapshots. |
| AWS Glue Data Catalog | Makes Iceberg metadata available to EMR and Redshift; it is not an ETL engine in this project. |
| Redshift Serverless + Spectrum | Reads Silver from the lakehouse and serves managed Gold tables through one SQL query plane. |
| Cosmos + dbt-redshift | Runs the dbt model and test graph under Airflow orchestration to build Gold. |
| Publication manifest | Records the exact source, Iceberg snapshots, dbt result, and row counts accepted for a published month. |

There is no Athena query path and no AWS Glue ETL job. Transient EMR on EC2 is
the only Spark compute path, while Redshift is the analytical serving path.

## How one monthly run works

1. The upstream producer lands the trip Parquet file and taxi-zone reference
   file in S3 with their SHA-256 metadata. This repository starts from that
   landing contract; it does not download or upload source data.
2. Airflow identifies the immutable source, creates one transient EMR cluster,
   and submits Bronze. Bronze preserves the accepted source rows and records
   the source run. The same cluster then runs Silver; Airflow requests its
   termination before dbt starts.
3. A second Spark job validates and deterministically deduplicates Bronze.
   Valid rows go to Silver; rejected rows go to quarantine with a reason.
4. Redshift Spectrum reads the Silver Iceberg table through Glue Data Catalog.
   Cosmos runs one tested dbt build that creates three dimensions, one fact,
   and two analytical marts as managed Redshift relations.
5. Reconciliation checks that all Bronze rows are accounted for and that the
   Silver and Gold fact counts agree.
6. Only a reconciled run receives an immutable publication manifest. A final
   bounded read verifies that the published Gold output is available to a
   consumer.

## Reliability and correctness

The pipeline is designed around a few observable guarantees rather than a
large framework:

- **Immutable source identity:** URI, SHA-256, object size, year, and month
  identify a source. Changed content for an existing month is rejected.
- **Safe reruns:** the same completed source reuses its committed Iceberg
  snapshots and successful dbt result before revalidating the serving output.
- **Deterministic row handling:** exact duplicate rows share a stable `row_id`;
  validation rules always send each Bronze row to either Silver or quarantine.
- **Cross-layer reconciliation:** `Bronze = Silver + quarantine` and
  `Silver = Gold fct_trips` must hold before publication.
- **Evidence-based publication:** the publication JSON ties the accepted
  source to exact Iceberg snapshots, dbt output, and reconciled row counts.

Detailed field-level rules, state transitions, and failure behavior live in
[runtime semantics](docs/SEMANTICS.md).

To learn the implementation end to end and verify that you can safely operate
and change it, follow the [codebase ownership and acceptance guide](docs/CODEBASE_OWNERSHIP_GUIDE.md).

## Gold data product

The consumer contract is six managed Redshift relations:

```text
dim_date
dim_operator
dim_zone
fct_trips
mart_hourly_zone_demand
mart_operator_metrics
```

The marts support demand analysis by pickup zone and hour, plus monthly
operator performance analysis. BI tools and SQL users consume these relations;
a vendor-specific dashboard is intentionally outside this repository.

## Repository guide

```text
etl/dags/             Airflow workflow and task dependencies
etl/spark_jobs/       Bronze and Silver Spark transformations
etl/dbt_project/      Redshift Gold models and tests
etl/orchestration/    Reconciliation, publication, and verification boundaries
terraform/            AWS infrastructure
docs/                 Architecture, semantics, deployment, and teardown guides
```

## Verify and deploy

Run the core local checks before planning infrastructure:

```powershell
venv\Scripts\python.exe -m black --check etl scripts tests
venv\Scripts\python.exe -m compileall -q etl scripts tests
venv\Scripts\python.exe -m pytest -p no:cacheprovider tests/unit -q
venv\Scripts\python.exe scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip --check

terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
```

For prerequisites, service quotas, Terraform state setup, deployment order,
the first bounded AWS run, retained evidence, and safe cleanup, follow the
[deployment runbook](docs/RUNBOOK.md). The [architecture document](docs/ARCHITECTURE.md)
explains the component boundaries, and the [teardown runbook](docs/TEARDOWN_RUNBOOK.md)
covers resource removal.

## Verification status

Local tests validate code, contracts, packaging, dbt parsing, and Terraform
configuration. They do not prove a live AWS deployment. MWAA, S3, transient
EMR on EC2, Iceberg commits, Redshift Serverless/Spectrum, rerun behavior,
schema evolution, and teardown remain **NOT VERIFIED** until the project
retains evidence from a bounded end-to-end AWS run.
