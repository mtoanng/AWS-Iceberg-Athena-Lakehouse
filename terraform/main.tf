terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

output "aws_account_id" {
  value       = data.aws_caller_identity.current.account_id
  description = "AWS account used by the approved Terraform run."
}

output "aws_region" {
  value       = var.aws_region
  description = "AWS region used by the approved Terraform run."
}

output "s3_bucket_name" {
  value       = aws_s3_bucket.lakehouse.id
  description = "Canonical lakehouse S3 bucket."
}

output "s3_landing_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/${var.landing_prefix}"
  description = "Landing prefix for monthly source objects."
}

output "s3_warehouse_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix}"
  description = "Iceberg warehouse root."
}

output "glue_database_name" {
  value       = { for name, database in aws_glue_catalog_database.namespace : name => database.name }
  description = "Canonical Glue Catalog namespaces for the NYC lakehouse."
}

output "emr_service_role_arn" {
  value       = aws_iam_role.emr_service.arn
  description = "EMR service role used by each transient Spark cluster."
}

output "emr_ec2_instance_profile" {
  value       = aws_iam_instance_profile.emr_ec2.name
  description = "EC2 instance profile used by each transient EMR cluster."
}

output "spark_script_prefix_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/spark_jobs"
  description = "S3 prefix for transient EMR PySpark entrypoints."
}

output "spark_package_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/${var.spark_package_s3_key}"
  description = "S3 URI for the shared EMR Python package."
}

output "redshift_serverless_host" {
  value       = aws_redshiftserverless_workgroup.gold.endpoint[0].address
  description = "Private Redshift Serverless endpoint used by dbt-redshift."
}

output "redshift_serverless_workgroup_name" {
  value       = aws_redshiftserverless_workgroup.gold.workgroup_name
  description = "Redshift Serverless workgroup used by dbt-redshift."
}

output "redshift_database_name" {
  value       = aws_redshiftserverless_namespace.gold.db_name
  description = "Redshift database containing the managed Gold schema."
}

output "mwaa_environment_name" {
  value       = aws_mwaa_environment.orchestration.name
  description = "Regular MWAA environment that owns pipeline orchestration."
}

output "mwaa_webserver_url" {
  value       = aws_mwaa_environment.orchestration.webserver_url
  description = "IAM-protected regular MWAA webserver URL."
}

output "publication_prefix_uri" {
  value       = "s3://${aws_s3_bucket.lakehouse.id}/manifests"
  description = "Month-partitioned publication-manifest prefix."
}

output "airflow_variables" {
  description = "Non-secret variables to import into regular MWAA before the first DAG run."
  value = {
    nyc_landing_uri              = "s3://${aws_s3_bucket.lakehouse.id}/${var.landing_prefix}"
    nyc_taxi_zone_uri            = "s3://${aws_s3_bucket.lakehouse.id}/${var.reference_prefix}/taxi_zone_lookup.csv"
    nyc_spark_script_prefix_uri  = "s3://${aws_s3_bucket.lakehouse.id}/spark_jobs"
    nyc_spark_package_uri        = "s3://${aws_s3_bucket.lakehouse.id}/${var.spark_package_s3_key}"
    nyc_emr_log_uri              = "s3://${aws_s3_bucket.lakehouse.id}/emr-logs"
    nyc_emr_service_role_arn     = aws_iam_role.emr_service.arn
    nyc_emr_ec2_instance_profile = aws_iam_instance_profile.emr_ec2.name
    nyc_emr_subnet_ids           = var.private_subnet_ids
    nyc_warehouse_uri            = "s3://${aws_s3_bucket.lakehouse.id}/${var.warehouse_prefix}"
    nyc_publication_prefix_uri   = "s3://${aws_s3_bucket.lakehouse.id}/manifests"
    redshift_host                = aws_redshiftserverless_workgroup.gold.endpoint[0].address
    redshift_database            = aws_redshiftserverless_namespace.gold.db_name
    redshift_workgroup_name      = aws_redshiftserverless_workgroup.gold.workgroup_name
    aws_account_id               = data.aws_caller_identity.current.account_id
    aws_region                   = var.aws_region
  }
}
