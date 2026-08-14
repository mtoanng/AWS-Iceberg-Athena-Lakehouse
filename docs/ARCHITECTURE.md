# Architecture

```text
S3 landing (input contract)
        |
        v
regular MWAA / Airflow 3
        |
        +--> transient EMR Bronze ----------+
        |                                  |
        +--> transient EMR Silver/Q --------+--> S3 Iceberg
                                               |
                                          Glue Data Catalog
                                                   |
                                               Spectrum
                                                   |
                                              Redshift Serverless
                                                      |
                                           Cosmos + dbt managed Gold
                                                      |
                                      reconcile -> publish -> verify
```

## Why the components are cohesive

Storage, compute, metadata, orchestration, and serving are intentionally
separate:

- S3/Iceberg is the canonical open data plane.
- Glue Data Catalog is metadata only; there are no Glue ETL jobs.
- One transient EMR on EC2 cluster performs both Spark steps per month. Its
  primary is On-Demand, its bounded Core fleet is Spot, and Airflow requests
  its termination before dbt starts.
- A failing Spark step terminates that cluster. Recovery is a new monthly DAG
  run against the same immutable source, not a retry of a sensor against a
  terminated cluster; the partition-scoped Iceberg writes make this safe.
- Redshift Spectrum is the only query path for Bronze, Silver, quarantine, and
  operational Iceberg tables.
- Cosmos Watcher exposes the dbt model/test graph in Airflow while one
  `dbt build` creates the managed serving model inside Redshift.
- MWAA installs Cosmos in the Airflow environment and its startup script creates
  an isolated dbt virtualenv; this preserves Watcher mode without mixing dbt and
  Airflow's conflicting dependency sets.
- MWAA controls stage ordering but contains no transformation logic.
- Reconciliation and verification use Redshift Data API; there is no second
  query engine.

## Network and authentication

MWAA and Redshift Serverless use the same existing VPC and two private subnets.
MWAA 3.2.1 exposes its IAM-authorized UI publicly while worker Task API traffic
uses the service-managed private endpoint (`PUBLIC_AND_PRIVATE`).
Redshift accepts port 5439 only from the MWAA security group. MWAA uses its
execution role for AWS APIs and dbt uses Redshift Serverless IAM-role
authentication. Terraform creates the corresponding password-disabled Redshift
IAM user and grants only external-schema reads plus Gold schema create/usage.

Private subnets must provide required AWS API and Python package access through
NAT and/or VPC endpoints. This network is an explicit deployment prerequisite,
not provisioned by this repository. Terraform adds the EMR v2 managed-policy
tag to the selected VPC/subnets so the EMR service role can create its
short-lived managed security groups and instances there.

## State boundaries

The only mutable operational state is `ops.source_run_manifest`. It records
the immutable source identity, stable run ID, current Bronze/Silver state,
counts, snapshots, and failure details.

Publication is separate durable release evidence. It is written only after dbt
and both reconciliation invariants pass. Verification reads the published
contract as a consumer would.

## Cost boundary

Each EMR cluster is terminated after the Silver step; step failure requests
termination and a 15-minute idle policy is a safety net. Redshift Serverless
bills for active work. Regular MWAA has a provisioned baseline cost, so
Terraform defaults to the smallest chosen class and supplies a review-only
bounded teardown plan. No teardown is applied automatically.
