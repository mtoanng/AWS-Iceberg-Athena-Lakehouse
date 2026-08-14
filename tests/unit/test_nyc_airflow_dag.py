"""DAG import and topology test without launching an Airflow service on Windows."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).parents[2] / "etl" / "dags" / "nyc_hvfhs_monthly_dag.py"


def _fake_airflow_modules(monkeypatch):
    current_dag: list[FakeDAG] = []

    def fake_package(name: str):
        package = types.ModuleType(name)
        package.__path__ = []
        return package

    class FakeParam:
        def __init__(self, default, **kwargs):
            self.default = default
            self.kwargs = kwargs

    class FakeDAG:
        def __init__(self, *, dag_id, params, **kwargs):
            self.dag_id = dag_id
            self.params = params
            self.kwargs = kwargs
            self.tasks = []

        def __enter__(self):
            current_dag.append(self)
            return self

        def __exit__(self, *_):
            current_dag.pop()

    class FakeOperator:
        def __init__(self, *, task_id, **kwargs):
            self.task_id = task_id
            self.kwargs = kwargs
            self.downstream_task_ids = set()
            current_dag[-1].tasks.append(self)

        def __rshift__(self, other):
            self.downstream_task_ids.add(other.task_id)
            return other

    class FakeDbtTaskGroup(FakeOperator):
        def __init__(self, *, group_id, **kwargs):
            super().__init__(task_id=group_id, **kwargs)

    class FakeConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeVariable:
        @staticmethod
        def get(_):
            raise AssertionError("DAG import must not resolve Airflow Variables")

    modules = {
        "airflow": fake_package("airflow"),
        "airflow.providers": fake_package("airflow.providers"),
        "airflow.providers.standard": fake_package("airflow.providers.standard"),
        "airflow.providers.standard.operators": fake_package(
            "airflow.providers.standard.operators"
        ),
        "airflow.providers.amazon": fake_package("airflow.providers.amazon"),
        "airflow.providers.amazon.aws": fake_package("airflow.providers.amazon.aws"),
        "airflow.providers.amazon.aws.operators": fake_package(
            "airflow.providers.amazon.aws.operators"
        ),
        "airflow.sdk": types.SimpleNamespace(
            DAG=FakeDAG, Param=FakeParam, Variable=FakeVariable
        ),
        "airflow.providers.standard.operators.python": types.SimpleNamespace(
            PythonOperator=FakeOperator
        ),
        "airflow.providers.standard.operators.trigger_dagrun": types.SimpleNamespace(
            TriggerDagRunOperator=FakeOperator
        ),
        "airflow.providers.amazon.aws.operators.emr": types.SimpleNamespace(
            EmrAddStepsOperator=FakeOperator,
            EmrCreateJobFlowOperator=FakeOperator,
            EmrTerminateJobFlowOperator=FakeOperator,
        ),
        "airflow.providers.amazon.aws.sensors": fake_package(
            "airflow.providers.amazon.aws.sensors"
        ),
        "airflow.providers.amazon.aws.sensors.emr": types.SimpleNamespace(
            EmrStepSensor=FakeOperator
        ),
        "cosmos": types.SimpleNamespace(DbtTaskGroup=FakeDbtTaskGroup),
        "cosmos.config": types.SimpleNamespace(
            ExecutionConfig=FakeConfig,
            ProfileConfig=FakeConfig,
            ProjectConfig=FakeConfig,
            RenderConfig=FakeConfig,
        ),
        "cosmos.constants": types.SimpleNamespace(
            ExecutionMode=types.SimpleNamespace(WATCHER="watcher"),
            InvocationMode=types.SimpleNamespace(SUBPROCESS="subprocess"),
            TestBehavior=types.SimpleNamespace(BUILD="build"),
        ),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_airflow_dag_import_and_manual_topology(monkeypatch) -> None:
    _fake_airflow_modules(monkeypatch)
    spec = importlib.util.spec_from_file_location("phase5_test_dag", DAG_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monthly = module.nyc_hvfhs_monthly_dag
    assert monthly.dag_id == "nyc_hvfhs_monthly"
    assert [task.task_id for task in monthly.tasks] == [
        "prepare_month",
        "create_emr_cluster",
        "bronze_ingestion_emr",
        "bronze_ingestion_complete",
        "silver_transform_emr",
        "silver_transform_complete",
        "terminate_emr_cluster",
        "dbt_build",
        "dbt_result_artifact",
        "reconciliation",
        "publication_manifest",
        "verification",
    ]
    assert monthly.params["year"].default == 2024
    assert monthly.params["month"].default == 1
    assert set(monthly.params) == {"year", "month"}
    assert monthly.tasks[0].downstream_task_ids == {"create_emr_cluster"}
    assert monthly.tasks[1].downstream_task_ids == {"bronze_ingestion_emr"}
    assert monthly.tasks[2].downstream_task_ids == {"bronze_ingestion_complete"}
    assert monthly.tasks[3].downstream_task_ids == {"silver_transform_emr"}
    assert monthly.tasks[4].downstream_task_ids == {"silver_transform_complete"}
    assert monthly.tasks[5].downstream_task_ids == {"terminate_emr_cluster"}
    assert monthly.tasks[6].downstream_task_ids == {"dbt_build"}
    assert all(
        monthly.tasks[index].kwargs["aws_conn_id"] is None for index in range(1, 7)
    )
    assert all(monthly.tasks[index].kwargs["retries"] == 0 for index in range(1, 7))
    job_flow = monthly.tasks[1].kwargs["job_flow_overrides"]
    assert job_flow["AutoTerminationPolicy"] == {"IdleTimeout": 900}
    assert job_flow["Tags"] == [
        {"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}
    ]
    assert job_flow["Instances"]["InstanceFleets"][0]["TargetOnDemandCapacity"] == 1
    spot_fleet = job_flow["Instances"]["InstanceFleets"][1]
    assert spot_fleet["TargetSpotCapacity"] == 1
    assert spot_fleet["LaunchSpecifications"]["SpotSpecification"] == {
        "TimeoutDurationMinutes": 10,
        "TimeoutAction": "SWITCH_TO_ON_DEMAND",
        "AllocationStrategy": "price-capacity-optimized",
    }
    assert monthly.tasks[7].downstream_task_ids == {"dbt_result_artifact"}
    assert monthly.tasks[8].downstream_task_ids == {"reconciliation"}
    assert monthly.tasks[9].downstream_task_ids == {"publication_manifest"}
    assert monthly.tasks[10].downstream_task_ids == {"verification"}
    assert all(
        "trigger_rule" not in monthly.tasks[index].kwargs for index in (8, 9, 10, 11)
    )
    assert monthly.tasks[9].kwargs["python_callable"].__name__ == "reconcile_month"
    assert monthly.tasks[10].kwargs["python_callable"].__name__ == "publish_month"
    assert monthly.tasks[11].kwargs["python_callable"].__name__ == "verify_month"
    dbt_group = monthly.tasks[7]
    assert dbt_group.kwargs["project_config"].kwargs["dbt_project_path"].name == (
        "dbt_project"
    )
    assert dbt_group.kwargs["profile_config"].kwargs["target_name"] == "redshift"
    assert dbt_group.kwargs["render_config"].kwargs["test_behavior"] == "build"
    assert dbt_group.kwargs["execution_config"].kwargs["execution_mode"] == "watcher"
    assert (
        "archive_cosmos_dbt_run_results"
        in dbt_group.kwargs["execution_config"].kwargs["setup_operator_args"][
            "callback"
        ]
    )
    assert set(dbt_group.kwargs["operator_args"]["vars"]) == {
        "source_year",
        "source_month",
    }
    assert {
        "REDSHIFT_HOST",
        "REDSHIFT_WORKGROUP_NAME",
        "REDSHIFT_DATABASE",
        "AWS_ACCOUNT_ID",
        "AWS_REGION",
    } == set(dbt_group.kwargs["operator_args"]["env"])

    backfill = module.nyc_hvfhs_four_month_backfill_dag
    assert [task.task_id for task in backfill.tasks] == [
        "prepare_backfill",
        "trigger_month_1",
        "trigger_month_2",
        "trigger_month_3",
        "trigger_month_4",
    ]
    assert backfill.params["month"].kwargs["maximum"] == 12
    assert backfill.tasks[0].downstream_task_ids == {"trigger_month_1"}
    assert backfill.tasks[1].downstream_task_ids == {"trigger_month_2"}
    assert backfill.tasks[2].downstream_task_ids == {"trigger_month_3"}
    assert backfill.tasks[3].downstream_task_ids == {"trigger_month_4"}
