"""Static proof that the final runtime has no retired ETL paths."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).parents[2]


def _active_text() -> str:
    paths = [
        ROOT / "etl",
        ROOT / "terraform",
        ROOT / ".github",
        ROOT / "requirements-airflow.txt",
        ROOT / "requirements-ci.txt",
    ]
    return "\n".join(
        path.read_text(encoding="utf-8")
        for root in paths
        for path in ([root] if root.is_file() else root.rglob("*"))
        if path.is_file()
        and path.suffix in {".py", ".tf", ".yml", ".yaml", ".txt"}
        and "__pycache__" not in path.parts
    ).lower()


def test_great_expectations_and_glue_etl_are_absent_from_active_runtime() -> None:
    text = _active_text()
    assert not (
        ROOT / "etl" / "spark_jobs" / "nyc_great_expectations_checkpoint.py"
    ).exists()
    assert not (ROOT / "etl" / "glue_jobs").exists()
    assert "great" + "_expectations" not in text
    assert "great expectations" not in text
    assert "gluejoboperator" not in text
    assert "interactive session" not in text
    assert "dbt" + "-glue" not in text
    assert "astronomer-cosmos==1.15.0" in text
    assert "dbttaskgroup(" in text
    assert "aws_instance.airflow_runner" not in text


def test_only_bronze_and_silver_are_monthly_emr_processing_jobs() -> None:
    dag = (ROOT / "etl" / "dags" / "nyc_hvfhs_monthly_dag.py").read_text(
        encoding="utf-8"
    )
    assert dag.count("EmrAddStepsOperator(") == 2
    assert dag.count("EmrStepSensor(") == 2
    assert "EmrCreateJobFlowOperator(" in dag
    assert "EmrTerminateJobFlowOperator(" in dag
    assert "EmrServerlessStartJobOperator" not in dag
    assert "bronze_ingestion_emr" in dag
    assert "silver_transform_emr" in dag
    assert "nyc_quality_checkpoint.py" not in dag
    assert "nyc_publish_manifest.py" not in dag
