"""Airflow 3 orchestration for the bounded NYC HVFHV lakehouse."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import re
from urllib.parse import urlparse

import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, Variable
from cosmos import DbtTaskGroup
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, InvocationMode, TestBehavior

from etl.contracts.nyc_hvfhs_identity import identity_policy_version
from etl.orchestration.nyc_hvfhs_cosmos import require_dbt_result_artifact
from etl.orchestration.nyc_hvfhs_publication import publish_month
from etl.orchestration.nyc_hvfhs_reconciliation import reconcile_month
from etl.orchestration.nyc_hvfhs_runs import (
    MonthlyRunRequest,
    sequential_backfill_requests,
)
from etl.orchestration.nyc_hvfhs_verification import verify_month
from etl.sources.nyc_hvfhs import (
    SourceFile,
    monthly_trip_filename,
    stable_run_id,
    validate_landed_source,
)


MONTHLY_DAG_ID = "nyc_hvfhs_monthly"
BACKFILL_DAG_ID = "nyc_hvfhs_four_month_backfill"
DBT_PROJECT_PATH = Path(__file__).resolve().parents[1] / "dbt_project"
DBT_PROFILES_PATH = DBT_PROJECT_PATH / "profiles.yml"
DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/dbt_venv/bin/dbt")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

EMR_SCRIPT_PREFIX_URI = "{{ var.value.nyc_spark_script_prefix_uri }}"
EMR_PACKAGE_URI = "{{ var.value.nyc_spark_package_uri }}"
EMR_LOG_URI = "{{ var.value.nyc_emr_log_uri }}"
EMR_SERVICE_ROLE_ARN = "{{ var.value.nyc_emr_service_role_arn }}"
EMR_EC2_INSTANCE_PROFILE = "{{ var.value.nyc_emr_ec2_instance_profile }}"
EMR_SUBNET_IDS = "{{ var.json.nyc_emr_subnet_ids }}"


def _spark_submit_command(script_name: str, arguments: list[str]) -> str:
    """Build the one Spark submit command shared by the transient EMR steps."""

    quoted_arguments = " ".join(arguments)
    return (
        "spark-submit "
        f"--py-files {EMR_PACKAGE_URI} "
        "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar "
        "--conf spark.driver.cores=1 "
        "--conf spark.driver.memory=3g "
        "--conf spark.executor.cores=1 "
        "--conf spark.executor.memory=3g "
        "--conf spark.dynamicAllocation.initialExecutors=1 "
        "--conf spark.dynamicAllocation.maxExecutors=3 "
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        "--conf spark.sql.defaultCatalog=glue_catalog "
        "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
        "--conf spark.sql.catalog.glue_catalog.warehouse={{ var.value.nyc_warehouse_uri }} "
        "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
        "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
        f"{EMR_SCRIPT_PREFIX_URI}/{script_name} {quoted_arguments}"
    )


def _emr_step(script_name: str, arguments: list[str]) -> dict[str, object]:
    return {
        "Name": script_name.removesuffix(".py"),
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["bash", "-c", _spark_submit_command(script_name, arguments)],
        },
    }


def _emr_job_flow() -> dict[str, object]:
    """Return the small, single-run EMR cluster specification.

    The primary node is deliberately On-Demand. The one-worker core fleet uses
    capacity-aware Spot and falls back only while the cluster is provisioning.
    Each Spark step requests cluster termination on failure; the idle policy is
    a second guard if orchestration is interrupted before a step is submitted.
    """

    return {
        "Name": "nyc-hvfhs-{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
        "ReleaseLabel": "emr-6.15.0",
        "Applications": [{"Name": "Spark"}],
        "LogUri": EMR_LOG_URI,
        "VisibleToAllUsers": True,
        "ServiceRole": EMR_SERVICE_ROLE_ARN,
        "JobFlowRole": EMR_EC2_INSTANCE_PROFILE,
        "Tags": [
            {
                "Key": "for-use-with-amazon-emr-managed-policies",
                "Value": "true",
            }
        ],
        "AutoTerminationPolicy": {"IdleTimeout": 900},
        "Instances": {
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2SubnetIds": EMR_SUBNET_IDS,
            "InstanceFleets": [
                {
                    "Name": "Primary On-Demand",
                    "InstanceFleetType": "MASTER",
                    "TargetOnDemandCapacity": 1,
                    "InstanceTypeConfigs": [
                        {"InstanceType": "m5.xlarge", "WeightedCapacity": 1}
                    ],
                },
                {
                    "Name": "Core Spot",
                    "InstanceFleetType": "CORE",
                    "TargetSpotCapacity": 1,
                    "InstanceTypeConfigs": [
                        {"InstanceType": "m5.xlarge", "WeightedCapacity": 1},
                        {"InstanceType": "m5a.xlarge", "WeightedCapacity": 1},
                    ],
                    "LaunchSpecifications": {
                        "SpotSpecification": {
                            "TimeoutDurationMinutes": 10,
                            "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                            "AllocationStrategy": "price-capacity-optimized",
                        }
                    },
                },
            ],
        },
    }


def _s3_identity(uri: str) -> tuple[str, int]:
    """Read upstream-provided immutable identity from a landed object."""

    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError(f"Expected a complete S3 URI, got {uri!r}")
    head = boto3.client("s3").head_object(Bucket=parsed.netloc, Key=key)
    checksum = str(head.get("Metadata", {}).get("sha256", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ValueError(f"Landed object is missing SHA-256 metadata: {uri}")
    size = int(head.get("ContentLength", 0))
    if size <= 0:
        raise ValueError(f"Landed object is empty: {uri}")
    return checksum, size


def _prepare_month(year: int, month: int) -> dict[str, object]:
    """Bind a requested month to the object identity already landed in S3."""

    request = MonthlyRunRequest(year=int(year), month=int(month))
    source_uri = (
        f"{Variable.get('nyc_landing_uri').rstrip('/')}/"
        f"{monthly_trip_filename(request.year, request.month)}"
    )
    checksum, size_bytes = _s3_identity(source_uri)
    source = SourceFile(
        source_year=request.year,
        source_month=request.month,
        source_uri=source_uri,
        source_checksum=checksum,
        source_size_bytes=size_bytes,
    )
    validate_landed_source(source)
    taxi_zone_uri = Variable.get("nyc_taxi_zone_uri")
    taxi_zone_checksum, _ = _s3_identity(taxi_zone_uri)
    return {
        "run_id": stable_run_id(source),
        "source_year": source.source_year,
        "source_month": source.source_month,
        "source_uri": source.source_uri,
        "source_checksum": source.source_checksum,
        "source_size_bytes": source.source_size_bytes,
        "identity_policy_version": identity_policy_version(request.year),
        "taxi_zone_uri": taxi_zone_uri,
        "taxi_zone_checksum": taxi_zone_checksum,
    }


def _monthly_params() -> dict[str, Param]:
    return {
        "year": Param(2024, type="integer", minimum=2019, maximum=2099),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    }


with DAG(
    dag_id=MONTHLY_DAG_ID,
    description="One immutable NYC TLC month from Bronze through published Gold.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=_monthly_params(),
    render_template_as_native_obj=True,
    tags=["nyc", "hvfhs", "iceberg", "manual"],
) as nyc_hvfhs_monthly_dag:
    prepare_month = PythonOperator(
        task_id="prepare_month",
        python_callable=_prepare_month,
        op_kwargs={"year": "{{ params.year }}", "month": "{{ params.month }}"},
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=_emr_job_flow(),
        aws_conn_id=None,
        retries=0,
    )

    bronze_ingestion = EmrAddStepsOperator(
        task_id="bronze_ingestion_emr",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=[
            _emr_step(
                "nyc_bronze_ingestion.py",
                [
                    "--SOURCE_URI",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_uri'] }}",
                    "--SOURCE_YEAR",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
                    "--SOURCE_MONTH",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
                    "--SOURCE_CHECKSUM",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_checksum'] }}",
                    "--INGESTION_RUN_ID",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
                    "--TAXI_ZONE_URI",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['taxi_zone_uri'] }}",
                    "--TAXI_ZONE_CHECKSUM",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['taxi_zone_checksum'] }}",
                    "--SOURCE_SIZE_BYTES",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_size_bytes'] }}",
                ],
            )
        ],
        aws_conn_id=None,
        retries=0,
    )

    bronze_complete = EmrStepSensor(
        task_id="bronze_ingestion_complete",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ ti.xcom_pull(task_ids='bronze_ingestion_emr')[0] }}",
        aws_conn_id=None,
        retries=0,
    )

    silver_transform = EmrAddStepsOperator(
        task_id="silver_transform_emr",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        steps=[
            _emr_step(
                "nyc_silver_transform.py",
                [
                    "--SOURCE_YEAR",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
                    "--SOURCE_MONTH",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
                    "--INGESTION_RUN_ID",
                    "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
                ],
            )
        ],
        aws_conn_id=None,
        retries=0,
    )

    silver_complete = EmrStepSensor(
        task_id="silver_transform_complete",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        step_id="{{ ti.xcom_pull(task_ids='silver_transform_emr')[0] }}",
        aws_conn_id=None,
        retries=0,
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ ti.xcom_pull(task_ids='create_emr_cluster') }}",
        aws_conn_id=None,
        retries=0,
    )

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            install_dbt_deps=False,
        ),
        profile_config=ProfileConfig(
            profile_name="nyc_hvfhs_lakehouse",
            target_name="redshift",
            profiles_yml_filepath=DBT_PROFILES_PATH,
        ),
        render_config=RenderConfig(
            test_behavior=TestBehavior.BUILD,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.WATCHER,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            setup_operator_args={
                "callback": "etl.orchestration.nyc_hvfhs_cosmos.archive_cosmos_dbt_run_results"
            },
        ),
        operator_args={
            "vars": {
                "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
                "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            },
            "env": {
                "REDSHIFT_HOST": "{{ var.value.redshift_host }}",
                "REDSHIFT_WORKGROUP_NAME": "{{ var.value.redshift_workgroup_name }}",
                "REDSHIFT_DATABASE": "{{ var.value.redshift_database }}",
                "AWS_ACCOUNT_ID": "{{ var.value.aws_account_id }}",
                "AWS_REGION": "{{ var.value.aws_region }}",
            },
        },
    )

    dbt_result_artifact = PythonOperator(
        task_id="dbt_result_artifact",
        python_callable=require_dbt_result_artifact,
        op_kwargs={
            "publication_prefix_uri": "{{ var.value.nyc_publication_prefix_uri }}",
            "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "run_id": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
        },
    )

    reconciliation = PythonOperator(
        task_id="reconciliation",
        python_callable=reconcile_month,
        op_kwargs={
            "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "ingestion_run_id": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            "redshift_database": "{{ var.value.redshift_database }}",
            "redshift_workgroup_name": "{{ var.value.redshift_workgroup_name }}",
        },
    )

    publication_manifest = PythonOperator(
        task_id="publication_manifest",
        python_callable=publish_month,
        op_kwargs={
            "audit": "{{ ti.xcom_pull(task_ids='prepare_month') }}",
            "reconciliation": "{{ ti.xcom_pull(task_ids='reconciliation') }}",
            "dbt_result_uri": "{{ ti.xcom_pull(task_ids='dbt_result_artifact') }}",
            "publication_prefix_uri": "{{ var.value.nyc_publication_prefix_uri }}",
            "redshift_database": "{{ var.value.redshift_database }}",
        },
    )

    verification = PythonOperator(
        task_id="verification",
        python_callable=verify_month,
        op_kwargs={
            "source_year": "{{ ti.xcom_pull(task_ids='prepare_month')['source_year'] }}",
            "source_month": "{{ ti.xcom_pull(task_ids='prepare_month')['source_month'] }}",
            "ingestion_run_id": "{{ ti.xcom_pull(task_ids='prepare_month')['run_id'] }}",
            "publication": "{{ ti.xcom_pull(task_ids='publication_manifest') }}",
            "redshift_database": "{{ var.value.redshift_database }}",
            "redshift_workgroup_name": "{{ var.value.redshift_workgroup_name }}",
        },
    )

    (
        prepare_month
        >> create_emr_cluster
        >> bronze_ingestion
        >> bronze_complete
        >> silver_transform
        >> silver_complete
        >> terminate_emr_cluster
        >> dbt_build
        >> dbt_result_artifact
        >> reconciliation
        >> publication_manifest
        >> verification
    )


with DAG(
    dag_id=BACKFILL_DAG_ID,
    description="Trigger four bounded monthly runs sequentially.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=_monthly_params(),
    render_template_as_native_obj=True,
    tags=["nyc", "hvfhs", "iceberg", "manual", "backfill"],
) as nyc_hvfhs_four_month_backfill_dag:

    def _prepare_backfill(year: int, month: int) -> list[dict[str, int]]:
        return [
            {"year": request.year, "month": request.month}
            for request in sequential_backfill_requests(int(year), int(month))
        ]

    prepare_backfill = PythonOperator(
        task_id="prepare_backfill",
        python_callable=_prepare_backfill,
        op_kwargs={"year": "{{ params.year }}", "month": "{{ params.month }}"},
    )
    triggers = [
        TriggerDagRunOperator(
            task_id=f"trigger_month_{index + 1}",
            trigger_dag_id=MONTHLY_DAG_ID,
            conf=f"{{{{ ti.xcom_pull(task_ids='prepare_backfill')[{index}] }}}}",
            wait_for_completion=True,
        )
        for index in range(4)
    ]
    prepare_backfill >> triggers[0] >> triggers[1] >> triggers[2] >> triggers[3]


nyc_hvfhs_monthly_dag.doc_md = """
# NYC HVFHV monthly orchestration

Trigger with `year` and `month`. The same immutable object identity may be
rerun safely; a changed URI, SHA-256, or byte size is rejected. Bronze owns
source checks, Silver owns validation/quarantine, dbt owns Gold tests, and
reconciliation owns the two cross-layer count invariants.
"""
