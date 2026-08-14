locals {
  spark_package_file = abspath("${path.module}/../${var.spark_package_path}")
}

resource "aws_s3_object" "spark_package" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = var.spark_package_s3_key
  source = local.spark_package_file
  etag   = filemd5(local.spark_package_file)
}

resource "aws_s3_object" "spark_script" {
  for_each = toset([
    "apply_nyc_2025_schema_evolution.py",
    "nyc_bronze_ingestion.py",
    "nyc_silver_transform.py",
    "verify_nyc_snapshot.py",
  ])

  bucket = aws_s3_bucket.lakehouse.id
  key    = "spark_jobs/${each.value}"
  source = "${path.module}/../etl/spark_jobs/${each.value}"
  etag   = filemd5("${path.module}/../etl/spark_jobs/${each.value}")
}

# Clusters are deliberately not Terraform resources. MWAA creates one transient
# EMR job flow per monthly run and terminates it after the Spark steps finish.
resource "aws_iam_role" "emr_service" {
  name = "${var.project_name}-${var.environment}-emr-service"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service_managed" {
  role       = aws_iam_role.emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

resource "aws_iam_role_policy" "emr_service_custom_ec2_profile" {
  name = "${var.project_name}-${var.environment}-pass-ec2-profile"
  role = aws_iam_role.emr_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "PassCustomEc2Profile"
      Effect   = "Allow"
      Action   = ["iam:PassRole"]
      Resource = aws_iam_role.emr_ec2.arn
      Condition = {
        StringLike = { "iam:PassedToService" = "ec2.amazonaws.com*" }
      }
    }]
  })
}

resource "aws_iam_role" "emr_ec2" {
  name = "${var.project_name}-${var.environment}-emr-ec2"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${var.project_name}-${var.environment}-emr-ec2"
  role = aws_iam_role.emr_ec2.name
}

resource "aws_iam_role_policy" "emr_ec2_lakehouse" {
  name = "${var.project_name}-${var.environment}-lakehouse-access"
  role = aws_iam_role.emr_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "BucketLocation"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
      },
      {
        Sid    = "ReadSourceReferenceAndArtifacts"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.landing_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/${var.reference_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/spark_jobs/*"
        ]
      },
      {
        Sid    = "ManageCanonicalTablesAndLogs"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:ListMultipartUploadParts",
          "s3:PutObject"
        ]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.warehouse_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/tmp/*",
          "${aws_s3_bucket.lakehouse.arn}/emr-logs/*"
        ]
      },
      {
        Sid    = "GlueCatalogIcebergMetadata"
        Effect = "Allow"
        Action = [
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition",
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:DeleteTable",
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:UpdateTable"
        ]
        Resource = "*"
      }
    ]
  })
}

# The VPC/subnets remain externally owned. This is the one tag required by the
# EMR v2 managed service policy to provision into them.
resource "aws_ec2_tag" "emr_vpc" {
  resource_id = var.vpc_id
  key         = "for-use-with-amazon-emr-managed-policies"
  value       = "true"
}

resource "aws_ec2_tag" "emr_private_subnet" {
  for_each    = toset(var.private_subnet_ids)
  resource_id = each.value
  key         = "for-use-with-amazon-emr-managed-policies"
  value       = "true"
}
