variable "aws_region" {
  type        = string
  description = "AWS region for the bounded demo."
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Short environment name used in resource names."
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{0,15}$", var.environment))
    error_message = "environment must be 1-16 lowercase letters, digits, or hyphens."
  }
}

variable "project_name" {
  type        = string
  description = "Resource prefix for the NYC HVFHV lakehouse."
  default     = "nyc-hvfhs-lakehouse"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,39}$", var.project_name))
    error_message = "project_name must be a short lowercase resource prefix."
  }
}

variable "s3_bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name; set in a private tfvars file."
}

variable "landing_prefix" {
  type        = string
  description = "S3 prefix for immutable source objects."
  default     = "landing"
}

variable "reference_prefix" {
  type        = string
  description = "S3 prefix for the Taxi Zone lookup."
  default     = "reference"
}

variable "warehouse_prefix" {
  type        = string
  description = "S3 prefix for Iceberg metadata and data files."
  default     = "warehouse"
}

variable "spark_package_path" {
  type        = string
  description = "Repository-root-relative deterministic PySpark zip produced before deployment."
  default     = "build/nyc_spark_jobs.zip"
}

variable "redshift_database_name" {
  type        = string
  description = "Database created inside the bounded Redshift Serverless namespace."
  default     = "lakehouse"
}

variable "spark_package_s3_key" {
  type        = string
  description = "S3 key for the shared transient EMR Python package."
  default     = "spark_jobs/nyc_spark_jobs.zip"
}

variable "vpc_id" {
  type        = string
  description = "VPC containing regular MWAA and Redshift Serverless."

  validation {
    condition     = can(regex("^vpc-[0-9a-f]+$", var.vpc_id))
    error_message = "vpc_id must be an AWS VPC identifier."
  }
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "Exactly two private subnets in distinct Availability Zones with required AWS/PyPI egress."

  validation {
    condition = (
      length(var.private_subnet_ids) == 2 &&
      length(distinct(var.private_subnet_ids)) == 2 &&
      alltrue([for id in var.private_subnet_ids : can(regex("^subnet-[0-9a-f]+$", id))])
    )
    error_message = "private_subnet_ids must contain two distinct AWS subnet identifiers."
  }
}

variable "mwaa_environment_class" {
  type        = string
  description = "Cost-bounded regular MWAA environment class."
  default     = "mw1.small"
}

variable "mwaa_max_workers" {
  type        = number
  description = "Maximum regular MWAA workers for the bounded pipeline."
  default     = 2

  validation {
    condition     = var.mwaa_max_workers >= 1 && var.mwaa_max_workers <= 5
    error_message = "mwaa_max_workers must be between 1 and 5."
  }
}

variable "mwaa_dag_s3_prefix" {
  type        = string
  description = "S3 prefix synchronized by regular MWAA as its DAG folder."
  default     = "airflow"

  validation {
    condition     = can(regex("^[a-zA-Z0-9!_.*'()=-]+(/[a-zA-Z0-9!_.*'()=-]+)*$", var.mwaa_dag_s3_prefix))
    error_message = "mwaa_dag_s3_prefix must be a safe, relative S3 prefix."
  }
}
