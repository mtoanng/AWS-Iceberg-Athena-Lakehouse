# End-to-end Terraform deployment runbook

This is the primary procedure for deploying and proving the bounded NYC HVFHV
lakehouse on AWS. Run commands from the repository root in PowerShell unless a
step says otherwise.

The repository starts at an upstream-owned S3 landing contract. It does not
download or upload source data. The deployed path is exactly:

```text
upstream S3 landing
-> regular Amazon MWAA / Airflow 3.2.1
-> transient EMR on EC2 / PySpark
-> S3 Iceberg + Glue Data Catalog
-> Redshift Serverless / Spectrum
-> dbt-managed Gold
-> reconciliation -> publication -> verification
```

All AWS writes and costs require explicit approval. Validation and planning are
read-only; do not run `terraform apply`, an EMR job, or an Airflow DAG merely to
complete local checks.

Read [the architecture](ARCHITECTURE.md) and [runtime semantics](SEMANTICS.md)
before deployment. The retention boundary and targeted removal procedure are
also summarized in the dedicated [teardown runbook](TEARDOWN_RUNBOOK.md).

## 0. Know the deployment boundary

Terraform creates and manages:

- one private, versioned, SSE-S3-encrypted S3 bucket;
- Bronze, Silver, and Ops Glue Data Catalog databases;
- EMR service/EC2 runtime roles, an instance profile, and Spark artifacts for
  transient clusters created by MWAA;
- one Redshift Serverless namespace/workgroup, Spectrum role, external schemas,
  Gold schema, and MWAA database grants;
- one regular MWAA environment, its execution role, security group, DAG files,
  dbt project, Spark entrypoints, and requirements file.

Terraform does **not** create:

- the VPC, private subnets, NAT gateway, or VPC endpoints (it adds only the
  required `for-use-with-amazon-emr-managed-policies=true` tag to the selected
  VPC and subnets);
- producer-side source delivery or checksum generation;
- a dashboard or another query engine;
- a remote Terraform backend.

MWAA and Redshift are ongoing cost-bearing resources even while no DAG runs.
EMR compute exists only while a monthly cluster is running; each successful
run terminates it, while step failure and a 15-minute idle policy are safety
guards. The bounded teardown removes compute/control/serving resources but
deliberately retains S3, Glue metadata, Iceberg data, and release evidence.

## 1. Prepare the workstation and AWS identity

Required tools:

| Tool | Supported project value | Verification |
| --- | --- | --- |
| PowerShell | 7 recommended | `$PSVersionTable.PSVersion` |
| Python | 3.12 | `python --version` |
| AWS CLI | v2 | `aws --version` |
| Terraform | 1.15.x | `terraform version` |
| Git | current stable | `git --version` |

Use an AWS profile or SSO session. Do not put access keys in `.env`, tfvars,
Airflow Variables, or the repository:

```powershell
$env:AWS_PROFILE = '<approved-profile>'
$env:AWS_REGION = 'us-east-1'
aws sts get-caller-identity
aws configure get region
```

Confirm the returned account and region before continuing. `us-east-1` is the
reference region and supports the selected MWAA, EMR on EC2, Iceberg, and
Redshift Serverless path. Stop if the identity is not the intended deployment
account.

The Terraform caller needs permission to manage the resources listed in
section 0, pass the two generated service roles, execute Redshift Data API
bootstrap statements, and read the Redshift-managed admin secret during those
statements. It also needs `ec2:CreateTags` on the approved VPC and two approved
subnets for the EMR-required tag. Runtime data access remains on the narrower
EMR, MWAA, and Redshift roles defined in Terraform.

### 1.1 AWS Console setup and access check

This project creates AWS resources with Terraform. Do **not** create the S3
bucket, MWAA environment, EMR cluster, Redshift workgroup, Glue databases, IAM
roles, or security groups manually in the Console. Use the Console only for
account access, preflight inspection, approving Terraform-created resources,
and running the Airflow workflow.

1. Sign in through the approved AWS IAM Identity Center/SSO portal or approved
   federation method. Do not create an IAM access key for this project.
2. In the AWS Console top-right Region selector, choose **US East (N. Virginia)
   `us-east-1`**. It must match `aws_region` in `terraform.tfvars`.
3. Open **IAM** → **Roles** and confirm that your federated role is the approved
   deployment role. If your organization separates provisioning and operating
   roles, obtain both before proceeding: the Terraform role and the MWAA UI
   operator role.
4. Open **Billing and Cost Management** → **Budgets** (or your organization's
   cost tool) and confirm that this bounded deployment is allowed. MWAA and
   Redshift Serverless create costs even when the DAG is idle.
5. Open **Service Quotas**, **VPC**, and **IAM** in separate browser tabs; the
   exact checks are in sections 5.1 and 5.2 below.

For the Airflow UI after deployment, the human operator must have
`airflow:CreateWebLoginToken` for this MWAA environment (normally supplied via
an organization-approved policy such as `AmazonMWAAWebServerAccess`).

## 2. Inspect the release candidate

```powershell
git branch --show-current
git status --short
git diff --check
```

Deploy a reviewed commit from the intended branch. A dirty worktree is allowed
only while deliberately testing a release candidate; record the commit SHA and
the reviewed diff before apply:

```powershell
git rev-parse HEAD
git diff --stat
```

Do not deploy Terraform state, plans, `.env`, source files, private endpoints,
or credentials. Repository hygiene checks enforce the tracked-file boundary.

## 3. Run every credential-independent gate

Create/activate the existing project virtual environment and install the pinned
CI requirements if necessary:

```powershell
python -m venv venv
venv\Scripts\python.exe -m pip install --requirement requirements-ci.txt
```

Run Python, contract, packaging, and hygiene checks:

```powershell
venv\Scripts\python.exe -m black --check etl scripts tests
venv\Scripts\python.exe -m flake8 etl scripts tests --extend-ignore=E203,E501,W503
venv\Scripts\python.exe -m compileall -q etl scripts tests
venv\Scripts\python.exe -m pytest -p no:cacheprovider tests/unit -q
venv\Scripts\python.exe scripts/check_repository_hygiene.py
venv\Scripts\python.exe scripts/package_spark_jobs.py `
  --output build/nyc_spark_jobs.zip --check
```

The Spark packager must print a SHA-256 and exactly these four entrypoints:

```text
apply_nyc_2025_schema_evolution.py
nyc_bronze_ingestion.py
nyc_silver_transform.py
verify_nyc_snapshot.py
```

MWAA installs Cosmos through `requirements-airflow.txt`. Its startup script
creates `/usr/local/airflow/dbt_venv` with the pinned dbt-redshift runtime;
Cosmos Watcher invokes that isolated `dbt` binary. Do not move dbt into the main
MWAA requirements file: Airflow 3.2.1's constraint set conflicts with dbt's
transitive dependencies.

Parse the dbt graph without contacting Redshift:

```powershell
$env:DBT_CI_REDSHIFT_HOST='127.0.0.1'
$env:DBT_CI_REDSHIFT_USER='ci'
$env:DBT_CI_REDSHIFT_PASSWORD='ci-not-used'
$env:DBT_CI_REDSHIFT_DATABASE='lakehouse'

venv\Scripts\dbt.exe parse `
  --project-dir etl/dbt_project `
  --profiles-dir etl/dbt_project `
  --target ci --no-partial-parse
venv\Scripts\python.exe scripts/verify_dbt_manifest.py
```

Do not treat `dbt compile` as an offline command for this adapter: depending on
the dbt/adapter version, it can still open a database connection. The first
live Cosmos `dbt build` in section 10 is the supported compilation and
execution proof against the deployed Redshift workgroup.

Expected dbt contract:

```text
6 models
37 data tests
2 sources
```

Finally validate Terraform without changing AWS:

```powershell
terraform -chdir=terraform fmt -check
terraform -chdir=terraform init -backend=false -input=false
terraform -chdir=terraform validate
```

Do not proceed if any gate fails. Local success proves syntax, contracts, DAG
topology, dbt graph parsing, deterministic packaging, and Terraform structure;
it does not prove live AWS integration.

## 4. Prepare a dedicated Terraform state

This repository uses local Terraform state. The state is deployment-critical
and may contain sensitive infrastructure metadata: never commit, email, or put
it in evidence. Back it up only to an approved encrypted location.

Initialize normally for the real deployment:

```powershell
terraform -chdir=terraform init -input=false
terraform -chdir=terraform workspace list
terraform -chdir=terraform workspace show
terraform -chdir=terraform state list
```

Do not reuse a workspace containing resources from another project. For the
first NYC deployment, create a dedicated workspace:

```powershell
terraform -chdir=terraform workspace new nyc-hvfhs-dev
terraform -chdir=terraform workspace show
terraform -chdir=terraform state list
```

If it already exists, select it instead:

```powershell
terraform -chdir=terraform workspace select nyc-hvfhs-dev
```

Pass criteria before the first plan:

```text
workspace = nyc-hvfhs-dev
terraform state list = empty
```

If `state list` shows legacy Glue jobs, EC2, Athena, Instacart, or any unrelated
resource, stop. Do not use `terraform state rm`, delete the state file, or apply
a destroy plan as an improvised migration. Select a clean workspace/backend.

## 5. Prepare and validate deployment inputs

Create the ignored local variables file only if it does not already exist:

```powershell
if (-not (Test-Path terraform/terraform.tfvars)) {
  Copy-Item terraform/terraform.tfvars.example terraform/terraform.tfvars
}
```

Set these values in `terraform/terraform.tfvars`:

| Variable | Required value |
| --- | --- |
| `aws_region` | `us-east-1` for the reference deployment |
| `environment` | `dev` |
| `project_name` | `nyc-hvfhs-lakehouse` |
| `s3_bucket_name` | A new globally unique private bucket name |
| `vpc_id` | Existing VPC containing the two subnets |
| `private_subnet_ids` | Exactly two private subnets in distinct AZs |
| `spark_package_path` | `build/nyc_spark_jobs.zip` |
| `mwaa_environment_class` | `mw1.small` |
| `mwaa_max_workers` | `2` |

Do not reuse a source/data bucket or project name from another architecture.
The selected S3 bucket name must match the producer's destination.

### 5.0 Console preflight: choose VPC and two private subnets

Use this section if you prefer the AWS Console to discover the values for
`terraform.tfvars`; section 5.2 then verifies the same facts with AWS CLI.

1. Open **VPC** → **Your VPCs** in `us-east-1`. Select the approved existing
   VPC and copy its `vpc-...` ID into `vpc_id`.
2. Choose **Subnets**, filter by that VPC ID, and select exactly two subnets in
   different Availability Zones. Copy the two `subnet-...` IDs into
   `private_subnet_ids`.
3. For each selected subnet, open its details and verify **Auto-assign public
   IPv4 address** is disabled and that **Available IPv4 addresses** is at least
   3. Do not select a public subnet merely because it has more free addresses.
4. Open the subnet's associated route table. Confirm either a `0.0.0.0/0` route
   through an approved NAT gateway, or an organization-approved endpoint/egress
   design that permits MWAA package installation and AWS service APIs.
5. Do not manually add the EMR tag, create an EMR security group, or modify
   routes. Terraform's reviewed plan adds the required tag only; EMR creates
   its temporary managed security groups at runtime.

Example final `terraform/terraform.tfvars` section:

```hcl
aws_region     = "us-east-1"
environment    = "dev"
project_name   = "nyc-hvfhs-lakehouse"
s3_bucket_name = "nyc-hvfhs-lakehouse-yourinitials-2026"

vpc_id = "vpc-0123456789abcdef0"
private_subnet_ids = [
  "subnet-0123456789abcdef0",
  "subnet-0fedcba9876543210",
]
```

### 5.1 Verify service quotas and current usage

Quota checks must use the same account and Region as Terraform. This deployment
needs the following headroom; higher default quotas elsewhere are not a reason
to skip checking the account's applied values:

| Service | Deployment demand | Pass criterion before plan |
| --- | --- | --- |
| Amazon MWAA | 1 environment, maximum 2 workers | At least 1 environment slot remains and the applied workers-per-environment quota is at least 2. |
| Amazon EC2 / EMR | 1 On-Demand Primary + 1 Spot Core (`m5.xlarge` or `m5a.xlarge`) | At least 4 On-Demand vCPUs and 4 Spot vCPUs for the selected families remain; both private subnets have at least 3 free IPv4 addresses. |
| Redshift Serverless | 1 namespace, 1 workgroup, 8 base RPUs | At least 1 namespace and workgroup slot remain and aggregate base-RPU headroom is at least 8. |
| IAM | 4 roles and 1 instance profile | At least 4 role slots and 1 instance-profile slot remain. |
| Amazon VPC | 2 Terraform security groups plus up to 2 EMR-managed groups | At least 4 security-group slots remain; both selected subnets must also have free IPv4 addresses. |
| Glue Data Catalog | 3 databases and 5 Iceberg tables | At least 3 database and 5 table slots remain. No Glue job quota is involved. |
| Amazon S3 | 1 general-purpose bucket | At least 1 bucket slot remains and the chosen bucket name is globally available. |

First list the applied quotas through **Service Quotas**. The caller needs
`servicequotas:ListServices`, `servicequotas:ListServiceQuotas`,
`servicequotas:GetServiceQuota`, and `servicequotas:GetAWSDefaultServiceQuota`.
An `AccessDeniedException` is a failed preflight, not proof that defaults apply:

```powershell
$AwsRegion = 'us-east-1'

$QuotaServices = aws service-quotas list-services `
  --region $AwsRegion `
  --output json | ConvertFrom-Json

$RequiredQuotaServices = $QuotaServices.Services | Where-Object {
  $_.ServiceName -match 'Managed Workflows|Amazon Elastic Compute Cloud|Elastic MapReduce|Redshift|Virtual Private Cloud|Identity and Access Management|Glue|Simple Storage Service'
}
$RequiredQuotaServices | Sort-Object ServiceName |
  Format-Table ServiceName, ServiceCode

foreach ($Service in $RequiredQuotaServices) {
  Write-Host "`n=== $($Service.ServiceName) [$($Service.ServiceCode)] ==="
  aws service-quotas list-service-quotas `
    --region $AwsRegion `
    --service-code $Service.ServiceCode `
    --query 'Quotas[].{Name:QuotaName,Applied:Value,Adjustable:Adjustable,Code:QuotaCode}' `
    --output table
}
```

Do not rely on quota values alone. Record current usage with read-only service
APIs so that existing resources are subtracted from the applied limits:

```powershell
aws mwaa list-environments --region $AwsRegion --output json
aws emr list-clusters --region $AwsRegion --active --output json
aws ec2 describe-instances --region $AwsRegion --filters Name=tag:aws:elasticmapreduce:job-flow-id,Values='*' --output json
aws redshift-serverless list-namespaces --region $AwsRegion --output json
aws redshift-serverless list-workgroups --region $AwsRegion --output json
aws iam get-account-summary --output json

aws glue get-databases `
  --region $AwsRegion `
  --query 'length(DatabaseList)' --output text

aws s3api list-buckets `
  --query 'length(Buckets)' --output text

aws ec2 describe-security-groups `
  --region $AwsRegion `
  --query 'length(SecurityGroups)' --output text
```

The likely binding quotas are EC2 running On-Demand and Spot vCPUs for the
selected instance families. Use the applied Service Quotas and current EC2
usage, not a published default. MWAA's documented defaults are 10
environments, 25 workers per environment, and 5 webservers per environment.
Redshift Serverless documents default limits of 25 namespaces and 25 workgroups.
These are reference defaults only; the applied account values above decide the
deployment.

Request quota increases before `terraform apply`; approval can take time. Do
not compensate for an exhausted quota by raising worker concurrency, sharing a
legacy namespace, or applying into another project's state.

#### Console alternative for quotas

1. Open **Service Quotas** → **AWS services** and select each service in the
   table above. Confirm the Console Region is `us-east-1` before reading a
   Regional quota.
2. For **Amazon EC2**, search for quotas containing `m5`, `m5a`, `Spot`, and
   `On-Demand`. Confirm capacity for at least four vCPUs in the applicable
   standard-instance On-Demand family and Spot family. If the selected instance
   family is unavailable, stop rather than changing the code from the Console.
3. For **Amazon Managed Workflows for Apache Airflow**, **Amazon Redshift
   Serverless**, **IAM**, **Amazon VPC**, and **AWS Glue**, compare applied
   quota values with the demands in the table. Check existing resources in each
   service Console as well; quota headroom is quota minus current usage.
4. To request an increase: open the individual quota → **Request quota
   increase** → enter only the required target value → submit. Wait for status
   **Approved** before Terraform apply. Do not submit a request merely because
   the default is unfamiliar.

### 5.2 Verify the existing network

Set temporary shell values matching tfvars:

```powershell
$VpcId = 'vpc-xxxxxxxx'
$PrivateSubnetIds = @('subnet-xxxxxxxx', 'subnet-yyyyyyyy')
$AwsRegion = 'us-east-1'
```

Inspect the VPC and both subnets:

```powershell
aws ec2 describe-vpcs `
  --region $AwsRegion `
  --vpc-ids $VpcId `
  --query 'Vpcs[].{VpcId:VpcId,State:State,Cidr:CidrBlock}'

aws ec2 describe-subnets `
  --region $AwsRegion `
  --subnet-ids $PrivateSubnetIds `
  --query 'Subnets[].{SubnetId:SubnetId,VpcId:VpcId,AZ:AvailabilityZone,State:State,PublicIp:MapPublicIpOnLaunch,FreeIPs:AvailableIpAddressCount}'
```

Pass criteria:

- both subnets are `available` and belong to `$VpcId`;
- the Availability Zones differ;
- `MapPublicIpOnLaunch` is `false`;
- each subnet has sufficient free addresses;
- its route table provides outbound access through a NAT gateway, or the VPC
  has the required endpoints plus a package-install path.

Inspect effective route tables in the VPC and identify the associations for
both subnet IDs:

```powershell
aws ec2 describe-route-tables `
  --region $AwsRegion `
  --filters "Name=vpc-id,Values=$VpcId" `
  --query 'RouteTables[].{RouteTableId:RouteTableId,Associations:Associations[].SubnetId,Routes:Routes[].{Destination:DestinationCidrBlock,NatGatewayId:NatGatewayId,GatewayId:GatewayId,State:State}}'
```

Default public subnets are not sufficient: MWAA worker ENIs do not receive
public IP addresses. The requirements installation needs access to the Airflow
constraint URL and Python package indexes. Redshift remains non-public. MWAA
uses `PUBLIC_AND_PRIVATE`: IAM-authorized users reach the UI publicly, while
worker Task API traffic uses the service-managed private endpoint.

## 6. Record the upstream S3 landing contract

The producer must automatically land these objects in the bucket selected in
tfvars before the first DAG run:

```text
landing/fhvhv_tripdata_2024-01.parquet
reference/taxi_zone_lookup.csv
```

Each object must be non-empty and carry a lowercase 64-character SHA-256 in S3
user metadata key `sha256`. This repository does not upload either object.

The first Terraform apply creates the bucket, so this object check is executed
after section 8 and before the first DAG run. Record the expected keys now;
after the bucket and producer destination exist, verify the contract:

```powershell
$Bucket = '<same-bucket-as-tfvars>'
$TripKey = 'landing/fhvhv_tripdata_2024-01.parquet'
$ZoneKey = 'reference/taxi_zone_lookup.csv'

$TripHead = aws s3api head-object --bucket $Bucket --key $TripKey | ConvertFrom-Json
$ZoneHead = aws s3api head-object --bucket $Bucket --key $ZoneKey | ConvertFrom-Json

if ($TripHead.ContentLength -le 0 -or $TripHead.Metadata.sha256 -notmatch '^[0-9a-f]{64}$') {
  throw 'Trip object violates the immutable landing contract.'
}
if ($ZoneHead.ContentLength -le 0 -or $ZoneHead.Metadata.sha256 -notmatch '^[0-9a-f]{64}$') {
  throw 'Taxi Zone object violates the immutable landing contract.'
}
```

The metadata digest is producer-owned provenance. The pipeline rejects missing
or changed identity; it does not silently repair producer metadata.

## 7. Create and review the Terraform plan

Build the Spark artifact again immediately before planning because Terraform
hashes and uploads that exact file:

```powershell
venv\Scripts\python.exe scripts/package_spark_jobs.py `
  --output build/nyc_spark_jobs.zip --check
terraform -chdir=terraform fmt -check
terraform -chdir=terraform validate
terraform -chdir=terraform plan -input=false -out=baseline.tfplan
terraform -chdir=terraform show -no-color baseline.tfplan
```

For a first deployment, fail the review if any planned action contains a
delete:

```powershell
$Plan = terraform -chdir=terraform show -json baseline.tfplan | ConvertFrom-Json
$Deletes = @(
  $Plan.resource_changes |
    Where-Object { $_.change.actions -contains 'delete' }
)
if ($Deletes.Count -gt 0) {
  $Deletes.address
  throw 'First-deployment plan contains delete actions; wrong state/workspace.'
}
```

Review these facts manually:

- account, region, workspace, project name, VPC, subnets, and bucket are exact;
- the plan creates MWAA, EMR runtime roles/instance profile, Redshift Serverless, Glue namespaces,
  S3 controls, IAM roles/policies, security groups, Redshift bootstrap SQL, and
  source artifacts;
- there is no Athena, Glue ETL job, EC2 Airflow runner, public Redshift endpoint,
  static access key, or broad `glue:*` policy;
- S3 has versioning, SSE-S3 encryption, public-access blocking, and
  `force_destroy=false`;
- no persistent EMR cluster is created; the DAG owns a bounded primary/core fleet,
  step-failure termination, and an idle safety timeout;
- the selected VPC and its two private subnets receive only the required EMR v2
  managed-policy tag; no route table, NAT, or endpoint is changed;
- no unrelated resource is updated, replaced, or destroyed.

The saved plan is ignored by Git and must not be committed. Apply the reviewed
plan file, not a newly generated implicit plan.

## 8. Apply infrastructure after cost approval

Only after explicit approval:

```powershell
terraform -chdir=terraform apply baseline.tfplan
```

MWAA creation can take a long time. Do not interrupt Terraform merely because
the environment remains in `CREATING`. If apply fails, preserve the state and
fix the reported cause; rerun `plan` and `apply`. Do not manually delete a
partially created S3 bucket or state file.

Record non-secret outputs:

```powershell
terraform -chdir=terraform output
$Bucket = terraform -chdir=terraform output -raw s3_bucket_name
$MwaaName = terraform -chdir=terraform output -raw mwaa_environment_name
$EmrServiceRoleArn = terraform -chdir=terraform output -raw emr_service_role_arn
$EmrEc2InstanceProfile = terraform -chdir=terraform output -raw emr_ec2_instance_profile
$RedshiftWorkgroup = terraform -chdir=terraform output -raw redshift_serverless_workgroup_name
```

### 8.1 Verify the AWS control plane

```powershell
aws s3api get-bucket-versioning --bucket $Bucket
aws s3api get-public-access-block --bucket $Bucket
aws s3api get-bucket-encryption --bucket $Bucket

foreach ($Database in @('bronze', 'silver', 'ops')) {
  aws glue get-database --region $AwsRegion --name $Database
}

aws iam get-role --role-name ($EmrServiceRoleArn.Split('/')[-1]) `
  --query 'Role.{Name:RoleName,Arn:Arn}'
aws iam get-instance-profile --instance-profile-name $EmrEc2InstanceProfile `
  --query 'InstanceProfile.{Name:InstanceProfileName,Roles:Roles[].RoleName}'

aws redshift-serverless get-workgroup `
  --region $AwsRegion --workgroup-name $RedshiftWorkgroup `
  --query 'workgroup.{Name:workgroupName,Status:status,Public:publiclyAccessible,BaseCapacity:baseCapacity}'

aws mwaa get-environment `
  --region $AwsRegion --name $MwaaName `
  --query 'Environment.{Name:Name,Status:Status,AirflowVersion:AirflowVersion,Access:WebserverAccessMode}'
```

Pass criteria:

```text
S3 versioning = Enabled
S3 public access = fully blocked
Glue databases = bronze, silver, ops
EMR service role and EC2 instance profile = present before a job
Redshift workgroup = AVAILABLE and publiclyAccessible = false
MWAA = AVAILABLE, Airflow 3.2.1, PUBLIC_AND_PRIVATE
```

## 9. Configure and inspect regular MWAA

Export the non-secret Airflow Variable map:

```powershell
terraform -chdir=terraform output -json airflow_variables |
  Set-Content -Encoding utf8 build/airflow-variables.json
Get-Content build/airflow-variables.json
```

Open the MWAA environment from the AWS console and launch the Airflow UI. The
UI is public-routed but still requires an authorized AWS identity. Import the
JSON under **Admin -> Variables**, or create the entries individually.

Console click path:

1. Open **Amazon MWAA** → **Environments** → select
   `nyc-hvfhs-lakehouse-dev` → wait until status is **Available**.
2. Choose **Open Airflow UI**. If AWS denies access, do not change the MWAA
   execution role: ask the account administrator for the human UI permission
   `airflow:CreateWebLoginToken` described in section 1.1.
3. In Airflow, open **Admin** → **Variables** → **Import Variables** → select
   `build/airflow-variables.json` → confirm the import. If the Airflow 3 UI
   labels this menu differently, use its Variables administration page; do not
   paste the JSON into a Connection or a secret field.
4. Return to **DAGs**, wait for both DAGs to appear, and keep them paused until
   section 10 confirms the landed source objects.

Required keys:

```text
aws_account_id
aws_region
nyc_landing_uri
nyc_taxi_zone_uri
nyc_spark_script_prefix_uri
nyc_spark_package_uri
nyc_emr_log_uri
nyc_emr_service_role_arn
nyc_emr_ec2_instance_profile
nyc_emr_subnet_ids
nyc_warehouse_uri
nyc_publication_prefix_uri
redshift_host
redshift_database
redshift_workgroup_name
```

These values are identifiers, not secrets. Do not add AWS keys or the
Redshift-managed admin password.

Wait for DAG synchronization, then verify:

- no DAG import errors;
- `nyc_hvfhs_monthly` is present and paused;
- `nyc_hvfhs_four_month_backfill` is present and paused;
- the monthly DAG has exactly this order:

```text
prepare_month
-> create_emr_cluster
-> bronze_ingestion_emr -> bronze_ingestion_complete
-> silver_transform_emr -> silver_transform_complete
-> terminate_emr_cluster
-> dbt_build (Cosmos Watcher task group)
-> dbt_result_artifact
-> reconciliation
-> publication_manifest
-> verification
```

If the DAG is absent, inspect MWAA DAG-processing logs. If requirements failed,
inspect MWAA scheduler/webserver logs and verify subnet egress before changing
package versions. If Cosmos cannot find dbt, inspect startup-script logs and
require `/usr/local/airflow/dbt_venv/bin/dbt --version` to succeed on each MWAA
component.

## 10. Prove one immutable month

Confirm the two landing objects again, then unpause and trigger
`nyc_hvfhs_monthly` with:

```json
{"year": 2024, "month": 1}
```

Observe the run in Airflow. Do not trigger the backfill first. Stop and diagnose
the first failed task; do not manually skip a release gate.

Expected responsibilities:

| Task | Required outcome |
| --- | --- |
| `prepare_month` | Reads URI, SHA-256 metadata, byte size, and creates the stable run ID. |
| `create_emr_cluster` | Creates the short-lived EMR cluster with one On-Demand Primary and one Spot Core worker. |
| `bronze_ingestion_emr` | Verifies input, writes one Bronze partition, and records its Iceberg snapshot. |
| `bronze_ingestion_complete` | Waits for the Bronze EMR step. |
| `silver_transform_emr` | Validates/deduplicates the month and writes Silver plus deterministic quarantine. |
| `silver_transform_complete` | Waits for the Silver EMR step. |
| `terminate_emr_cluster` | Requests cluster termination before Redshift/dbt work starts. |
| `dbt_build` | Cosmos runs one dbt build and exposes model/test states while producing exactly six managed Gold relations. |
| `dbt_result_artifact` | Confirms a checksummed dbt `run_results.json` in S3. |
| `reconciliation` | Enforces `Bronze = Silver + quarantine` and `Silver = Gold`. |
| `publication_manifest` | Writes or safely reuses one immutable release JSON. |
| `verification` | Reads the publication and rechecks Silver/Gold through Redshift. |

Retain the Airflow run ID, EMR cluster ID, and two EMR step IDs from task logs. In the
Redshift Query Editor v2, connect to the deployed workgroup/database with an
authorized admin identity and run:

```sql
select source_year, source_month, run_status,
       bronze_row_count, silver_row_count, quarantine_row_count,
       bronze_snapshot_id, silver_snapshot_id, quarantine_snapshot_id
from ops_external.source_run_manifest
where source_year = 2024 and source_month = 1;

select reason_code, count(*) as row_count
from silver_external.quarantine_trips
where _source_year = 2024 and _source_month = 1
group by reason_code
order by reason_code;

select table_name
from information_schema.tables
where table_schema = 'gold'
order by table_name;

select count(*) as silver_count
from silver_external.silver_trips
where source_year = 2024 and source_month = 1;

select count(*) as gold_count
from gold.fct_trips
where source_year = 2024 and source_month = 1;
```

Exactly these Gold relations must exist:

```text
dim_date
dim_operator
dim_zone
fct_trips
mart_hourly_zone_demand
mart_operator_metrics
```

Release pass criteria:

```text
Bronze > 0
Bronze = Silver + quarantine
Silver = Gold fct_trips
dbt results contain only success/pass
publication status = published
verification Silver = published Silver
verification Gold = published Gold
```

Copy `docs/CLOUD_EVIDENCE_TEMPLATE.md` into the final evidence folder and fill
only non-sensitive facts. Never retain account IDs, private endpoints,
credentials, source data, Terraform state, or saved plans.

## 11. Prove retry and deterministic rerun

Only after the baseline passes:

1. Clear only the read-only `verification` task and let it run again.
2. Confirm it reads the same publication and counts.
3. Trigger the monthly DAG again with the identical year/month.
4. Confirm source URI/checksum/size, stable run ID, row counts, row IDs,
   quarantine distribution, and open-layer snapshot IDs are unchanged.

Create two ignored evidence JSON files with this shape:

```json
{
  "source_uri": "s3://...",
  "source_checksum": "...",
  "source_size_bytes": 1,
  "source_year": 2024,
  "source_month": 1,
  "ingestion_run_id": "...",
  "identity_policy_version": "...",
  "bronze_row_count": 1,
  "silver_row_count": 1,
  "quarantine_row_count": 0,
  "gold_row_count": 1,
  "row_ids": ["..."],
  "quarantine_by_reason": {}
}
```

Then compare them:

```powershell
venv\Scripts\python.exe scripts/verify_monthly_rerun.py `
  build/month-first.json build/month-rerun.json
```

Test changed-source rejection only in an isolated test month/environment. Do
not overwrite or mutate the accepted production landing object to manufacture
this failure.

## 12. Run the four-month sequence

Confirm all four monthly objects already satisfy the S3 landing contract. The
starting month must be January through September so the bounded sequence does
not cross a year boundary.

Trigger `nyc_hvfhs_four_month_backfill` with the first month:

```json
{"year": 2024, "month": 1}
```

The controller waits for four monthly DAG runs sequentially. Accept the
backfill only when every month independently publishes and verifies. Do not
increase Airflow/EMR parallelism for this bounded proof.

## 13. Prove the approved 2025 Iceberg evolution

Run this only after retaining a successful 2024 Silver snapshot ID.

Submit `s3://<bucket>/spark_jobs/apply_nyc_2025_schema_evolution.py` as a step
on a short-lived EMR cluster using the Terraform output service role and EC2
instance profile, with the same Spark parameters used by the DAG. The required
parameters include:

```text
--py-files s3://<bucket>/spark_jobs/nyc_spark_jobs.zip
--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.defaultCatalog=glue_catalog
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.glue_catalog.warehouse=s3://<bucket>/warehouse
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Wait for `SUCCESS`, then run one landed 2025 month through the monthly DAG.
Submit `verify_nyc_snapshot.py` with these entrypoint arguments:

```text
--SNAPSHOT_ID <retained-2024-silver-snapshot-id>
--SOURCE_YEAR 2024
--SOURCE_MONTH 1
```

The verification job must read the retained 2024 snapshot with `VERSION AS OF`
after the current table has the nullable `cbd_congestion_fee` column. Record an
ignored evidence JSON matching `scripts/verify_schema_evolution.py`, then run:

```powershell
venv\Scripts\python.exe scripts/verify_schema_evolution.py `
  build/schema-evolution-evidence.json
```

Do not add a generic migration framework. This is the single bounded evolution
proof in the project.

## 14. Troubleshooting and safe recovery

| Symptom | Likely cause | Safe action |
| --- | --- | --- |
| First plan contains deletes | Wrong workspace or legacy state | Stop; select a clean NYC workspace/backend. |
| S3 bucket creation conflicts | Bucket name already exists globally or belongs to old state | Choose a new NYC bucket; do not import an unrelated bucket. |
| MWAA stays `CREATE_FAILED` | Private subnet egress, requirements install, or execution-role permission | Read MWAA environment and CloudWatch errors; fix network/IAM, then re-plan. |
| DAG import error | Missing uploaded module, bad dependency, or requirements failure | Inspect DAG-processing logs; do not skip the broken task. |
| EMR says Iceberg class not found | Missing bundled Iceberg JAR Spark parameter | Restore the documented `spark.jars` parameter. |
| EMR exceeds maximum capacity | Driver/executor sizing drifted from the bounded 1-core/3-GB profile | Restore the committed Spark parameters; do not raise capacity first. |
| EMR S3/Glue access denied | Runtime role prefix/catalog policy mismatch | Compare the failing ARN with `terraform/iam.tf`; change the narrow policy only if required. |
| dbt cannot connect | Subnet route, Redshift SG, IAM credentials, or database grant | Verify workgroup availability, port 5439 SG path from MWAA, and bootstrap statements. |
| Spectrum table missing | Bronze has not created the Iceberg table or Glue metadata is unavailable | Fix the first failing EMR/catalog step; do not create a second table manually. |
| Reconciliation mismatch | A layer is incomplete or consumer visibility differs | Stop publication, inspect counts/snapshots, and rerun the failed canonical stage. |
| Publication key conflict | Same stable run ID produced different logical release facts | Treat as integrity failure; never overwrite the publication object. |

For a partial Terraform apply, keep the state file and run `terraform plan`
again after fixing the root cause. Terraform is the infrastructure owner. Do
not compensate with ad-hoc console resources unless the change is immediately
represented in Terraform.

For a failed EMR Spark step, do not clear only its sensor or step task: the
cluster intentionally terminated. Fix the cause, then trigger a new monthly
DAG run with the same year/month. Bronze and Silver are partition-scoped and
the operational manifest protects immutable month identity. Never use a force
bypass; none is implemented.

## 15. Bounded teardown

First confirm the selected workspace and capture required evidence:

```powershell
terraform -chdir=terraform workspace show
terraform -chdir=terraform state list
```

Generate a review-only targeted destroy plan:

```powershell
.\scripts\teardown.ps1
terraform -chdir=terraform show -no-color bounded-destroy.tfplan
```

The plan may remove only cost-bearing MWAA, EMR runtime IAM, Redshift
Serverless, their security groups, and their service roles/policies. It must not
delete:

```text
aws_s3_bucket.lakehouse
aws_s3_bucket_versioning.lakehouse
aws_s3_bucket_server_side_encryption_configuration.lakehouse
aws_s3_bucket_public_access_block.lakehouse
aws_glue_catalog_database.namespace
canonical S3/Iceberg/publication objects
```

Only after separate approval:

```powershell
terraform -chdir=terraform apply bounded-destroy.tfplan
```

Run the read-only verifier:

```powershell
venv\Scripts\python.exe scripts/verify_teardown.py `
  --bucket $Bucket `
  --project-name nyc-hvfhs-lakehouse `
  --environment dev `
  --region $AwsRegion
```

Pass means cost-bearing MWAA/EMR/Redshift/IAM resources are absent, the
temporary prefix is empty, and retained S3/Glue canonical data still exists.
The pre-existing VPC/NAT boundary is outside this Terraform state and must be
handled by its owner.

## 16. Final acceptance checklist

Mark the deployment complete only when every applicable item is recorded:

```text
[ ] reviewed commit and clean/deliberately recorded worktree
[ ] all local, dbt, package, and Terraform gates pass
[ ] correct AWS account and us-east-1 selected
[ ] dedicated NYC Terraform workspace/state selected
[ ] first plan contains zero delete actions
[ ] two private subnets in distinct AZs have required egress
[ ] producer-owned trip and zone objects satisfy the immutable S3 contract
[ ] MWAA, transient EMR on EC2, Redshift Serverless, S3, and Glue control planes pass
[ ] both Airflow DAGs import without errors
[ ] one 2024 month completes all twelve tasks
[ ] Bronze = Silver + quarantine
[ ] Silver = Gold fct_trips
[ ] exactly six Gold relations exist
[ ] dbt artifact, publication SHA, and read-after-publish verification retained
[ ] cleared verification task succeeds
[ ] identical monthly rerun evidence passes the verifier
[ ] four-month sequence and 2025 evolution marked PASS or explicitly NOT VERIFIED
[ ] bounded teardown completed after evidence, or ongoing cost is explicitly accepted
```

Until a retained AWS run exists, describe live MWAA, EMR, Iceberg, Redshift,
rerun, evolution, and teardown results as **NOT VERIFIED**, even when every
local gate passes.
