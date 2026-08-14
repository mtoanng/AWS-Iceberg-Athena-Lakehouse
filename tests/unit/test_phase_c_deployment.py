"""Static Phase C deployment, packaging, authentication, and teardown contracts."""

from __future__ import annotations

import json
from pathlib import Path
from zipfile import ZipFile

from scripts.package_spark_jobs import build

ROOT = Path(__file__).parents[2]


def test_spark_package_is_deterministic_and_contains_runtime_contract(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    manifest_one = build(first)
    manifest_two = build(second)
    assert manifest_one["entrypoints"] == manifest_two["entrypoints"]
    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        manifest = json.loads(archive.read("spark_runtime_manifest.json"))
        assert "etl/iceberg/catalog.py" in archive.namelist()
        assert "etl/spark_jobs/nyc_bronze_ingestion.py" in archive.namelist()
        assert "etl/spark_jobs/verify_nyc_snapshot.py" in archive.namelist()
        assert "etl/orchestration/nyc_hvfhs_cosmos.py" not in archive.namelist()
        assert "etl/transforms/nyc_hvfhs.py" not in archive.namelist()
        assert manifest["namespaces"] == ["bronze", "silver", "ops"]
    snapshot_job = (ROOT / "etl" / "spark_jobs" / "verify_nyc_snapshot.py").read_text(
        encoding="utf-8"
    )
    assert "VERSION AS OF" in snapshot_job
    assert "SNAPSHOT_ID" in snapshot_job


def test_terraform_is_nyc_only_and_uses_mwaa_and_package_contract() -> None:
    terraform = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "terraform").glob("*.tf")
    )
    assert "instacart" not in terraform.lower()
    assert 'resource "aws_mwaa_environment"' in terraform
    assert '"PUBLIC_AND_PRIVATE"' in terraform
    assert 'resource "aws_iam_instance_profile" "emr_ec2"' in terraform
    assert 'resource "aws_instance"' not in terraform
    assert "aws_emrserverless_application" not in terraform
    assert "AmazonEMRServicePolicy_v2" in terraform
    assert "AmazonElasticMapReduceforEC2Role" not in terraform
    assert "aws_emr_cluster" not in terraform
    assert 'resource "aws_ec2_tag" "emr_vpc"' in terraform
    assert 'resource "aws_ec2_tag" "emr_private_subnet"' in terraform
    dag = (ROOT / "etl" / "dags" / "nyc_hvfhs_monthly_dag.py").read_text(
        encoding="utf-8"
    )
    assert "--py-files" in dag
    assert "spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar" in dag
    assert "spark.driver.cores=1" in dag
    assert "spark.executor.cores=1" in dag
    assert "spark.dynamicAllocation.maxExecutors=3" in dag
    assert "emr-logs/*" in terraform
    assert '"etl/dbt_project/${path}"' in terraform
    assert '!startswith(path, "target/")' in terraform
    assert 'path != ".user.yml"' in terraform
    assert 'aws_glue_catalog_database" "namespace' in terraform
    assert terraform.count('resource "aws_redshiftserverless_namespace"') == 1
    assert terraform.count('resource "aws_redshiftserverless_workgroup"') == 1
    assert terraform.count('resource "aws_iam_role" "redshift_spectrum"') == 1
    assert "manage_admin_password = true" in terraform
    assert "default_iam_role_arn" in terraform
    assert "warehouse_prefix}/bronze/*" in terraform
    assert "warehouse_prefix}/silver/*" in terraform
    assert "warehouse_prefix}/ops/*" in terraform
    assert "CREATE EXTERNAL SCHEMA IF NOT EXISTS bronze_external" in terraform
    assert "CREATE EXTERNAL SCHEMA IF NOT EXISTS silver_external" in terraform
    assert "CREATE EXTERNAL SCHEMA IF NOT EXISTS ops_external" in terraform
    assert "CREATE SCHEMA IF NOT EXISTS gold" in terraform
    assert "nyc_great_expectations_checkpoint.py" not in terraform
    assert "nyc_quality_checkpoint.py" not in terraform
    assert not (ROOT / "glue-user-policy.json").exists()
    assert "nyc_publish_manifest.py" not in terraform


def test_teardown_has_no_apply() -> None:
    teardown = (ROOT / "scripts" / "teardown.ps1").read_text(encoding="utf-8").lower()
    assert "'plan', '-destroy'" in teardown
    assert "terraform apply" not in teardown


def test_cloud_environment_has_no_static_key_contract() -> None:
    env = (ROOT / ".env.cloud.example").read_text(encoding="utf-8")
    assert "AWS_ACCESS_KEY_ID" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert "AIRFLOW_VAR_NYC_EMR_SERVICE_ROLE_ARN" in env
    assert "AIRFLOW_VAR_NYC_EMR_EC2_INSTANCE_PROFILE" in env
    assert "AIRFLOW_VAR_NYC_EMR_SUBNET_IDS" in env
    assert "EMR_SERVERLESS" not in env
    assert "AIRFLOW_VAR_REDSHIFT_WORKGROUP_NAME" in env


def test_airflow_runtime_uses_cosmos_watcher_for_dbt_graph() -> None:
    requirements = (ROOT / "requirements-airflow.txt").read_text(encoding="utf-8")
    assert "astronomer-cosmos==1.15.0" in requirements
    assert "dbt-redshift" not in requirements
    startup = (ROOT / "scripts" / "mwaa_startup.sh").read_text(encoding="utf-8")
    assert "dbt-core==1.10.19" in startup
    assert "dbt-redshift==1.10.2" in startup
    assert "/usr/local/airflow/dbt_venv" in startup
    terraform = (ROOT / "terraform" / "mwaa.tf").read_text(encoding="utf-8")
    assert 'resource "aws_s3_object" "mwaa_startup"' in terraform
    assert "startup_script_s3_path" in terraform
    assert "startup_script_s3_object_version" in terraform
    dag = (ROOT / "etl" / "dags" / "nyc_hvfhs_monthly_dag.py").read_text(
        encoding="utf-8"
    )
    assert "DbtTaskGroup(" in dag
    assert "ExecutionMode.WATCHER" in dag
    assert 'Path("/usr/local/airflow/dbt_venv/bin/dbt")' in dag
    assert "BashOperator" not in dag
