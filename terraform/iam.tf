resource "aws_iam_role" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_platform" {
  name = "${var.project_name}-${var.environment}-mwaa-platform"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublishAirflowMetrics"
        Effect   = "Allow"
        Action   = ["airflow:PublishMetrics"]
        Resource = "arn:aws:airflow:${var.aws_region}:${data.aws_caller_identity.current.account_id}:environment/${var.project_name}-${var.environment}"
      },
      {
        Sid      = "ReadMwaaSourceBucket"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:GetBucketPublicAccessBlock", "s3:GetBucketVersioning", "s3:GetEncryptionConfiguration", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
      },
      {
        Sid      = "ReadMwaaDagAndRequirements"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/${var.mwaa_dag_s3_prefix}/*"
      },
      {
        Sid      = "ReadAccountPublicAccessBlock"
        Effect   = "Allow"
        Action   = ["s3:GetAccountPublicAccessBlock"]
        Resource = "*"
      },
      {
        Sid    = "MwaaCloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:DescribeLogGroups",
          "logs:GetLogGroupFields",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetQueryResults",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:airflow-${var.project_name}-${var.environment}-*"
      },
      {
        Sid      = "MwaaCloudWatchMetrics"
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Sid      = "MwaaCeleryQueue"
        Effect   = "Allow"
        Action   = ["sqs:ChangeMessageVisibility", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl", "sqs:ReceiveMessage", "sqs:SendMessage"]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      },
      {
        Sid      = "MwaaManagedKeyForCelery"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*"]
        Resource = "*"
        Condition = {
          StringLike = {
            "kms:ViaService" = "sqs.${var.aws_region}.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_pipeline" {
  name = "${var.project_name}-${var.environment}-mwaa-pipeline"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CreateAndObserveTransientEmrClusters"
        Effect = "Allow"
        Action = [
          "elasticmapreduce:AddJobFlowSteps",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:DescribeStep",
          "elasticmapreduce:RunJobFlow",
          "elasticmapreduce:TerminateJobFlows"
        ]
        Resource = "*"
      },
      {
        Sid    = "ConnectToRedshiftServerlessForDbt"
        Effect = "Allow"
        Action = ["redshift-serverless:GetCredentials", "redshift-serverless:GetWorkgroup", "redshift-serverless:GetNamespace"]
        Resource = [
          aws_redshiftserverless_workgroup.gold.arn,
          aws_redshiftserverless_namespace.gold.arn
        ]
      },
      {
        Sid      = "UseRedshiftDataApiQueryPlane"
        Effect   = "Allow"
        Action   = ["redshift-data:DescribeStatement", "redshift-data:ExecuteStatement", "redshift-data:GetStatementResult"]
        Resource = "*"
      },
      {
        Sid      = "PassEmrServiceRole"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.emr_service.arn
        Condition = {
          StringLike = { "iam:PassedToService" = "elasticmapreduce.amazonaws.com*" }
        }
      },
      {
        Sid      = "PassEmrEc2Role"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = aws_iam_role.emr_ec2.arn
        Condition = {
          StringLike = { "iam:PassedToService" = "ec2.amazonaws.com*" }
        }
      },
      {
        Sid    = "CreateEmrCleanupServiceLinkedRole"
        Effect = "Allow"
        Action = ["iam:CreateServiceLinkedRole"]
        Resource = [
          "arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*",
          "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
        ]
        Condition = {
          StringEquals = {
            "iam:AWSServiceName" = [
              "elasticmapreduce.amazonaws.com",
              "spot.amazonaws.com"
            ]
          }
        }
      },
      {
        Sid      = "ListPipelinePrefixes"
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.lakehouse.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [
              "${var.landing_prefix}/*",
              "${var.reference_prefix}/*",
              "manifests/*"
            ]
          }
        }
      },
      {
        Sid    = "ReadLandingAndReference"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${aws_s3_bucket.lakehouse.arn}/${var.landing_prefix}/*",
          "${aws_s3_bucket.lakehouse.arn}/${var.reference_prefix}/*"
        ]
      },
      {
        Sid      = "PublishAndVerifyRunEvidence"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.lakehouse.arn}/manifests/*"
      }
    ]
  })
}
