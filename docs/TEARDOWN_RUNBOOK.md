# Bounded teardown

The repository never applies teardown automatically.

Generate a review-only plan:

```powershell
.\scripts\teardown.ps1
terraform -chdir=terraform show bounded-destroy.tfplan
```

The targets remove cost-bearing control/compute/serving resources:

- regular MWAA and its execution role;
- transient EMR service/EC2 roles and instance profile;
- Redshift Serverless and Spectrum role;
- their security groups and inline policies.

The plan intentionally retains the private S3 bucket, Glue namespaces,
canonical Iceberg objects, publication JSON, and dbt evidence. Because
`force_destroy=false`, Terraform cannot silently recurse through retained S3
data.

Only after separate approval:

```powershell
terraform -chdir=terraform apply bounded-destroy.tfplan
```

Then run the read-only verifier:

```powershell
venv\Scripts\python.exe scripts/verify_teardown.py `
  --bucket <bucket>
```

Pass means canonical S3/Glue data remains, the temporary prefix is empty, and
MWAA/EMR/Redshift/IAM compute resources are absent.
