"""Build the deterministic shared Python artifact used by transient EMR jobs.

Usage: ``python scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip``.
The resulting zip is uploaded to ``spark_jobs/nyc_spark_jobs.zip`` and supplied
to EMR with Spark's ``--py-files`` argument.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).parents[1]
RUNTIME_DEPENDENCIES: dict[str, str] = {}


def _files() -> list[Path]:
    files = [ROOT / "etl" / "__init__.py"]
    for directory in ("contracts", "iceberg", "sources", "spark_jobs"):
        files.extend((ROOT / "etl" / directory).glob("*.py"))
    return sorted(path for path in files if path.is_file())


def build(output: Path) -> dict[str, object]:
    output.parent.mkdir(parents=True, exist_ok=True)
    entries = [(path, path.relative_to(ROOT).as_posix()) for path in _files()]
    manifest = {
        "artifact": "nyc_spark_jobs",
        "format": "zip",
        "entrypoints": [
            "etl/spark_jobs/apply_nyc_2025_schema_evolution.py",
            "etl/spark_jobs/nyc_bronze_ingestion.py",
            "etl/spark_jobs/nyc_silver_transform.py",
            "etl/spark_jobs/verify_nyc_snapshot.py",
        ],
        "dependencies": RUNTIME_DEPENDENCIES,
        "catalog": "glue_catalog",
        "namespaces": ["bronze", "silver", "ops"],
        "artifact_s3_key": "spark_jobs/nyc_spark_jobs.zip",
    }
    with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path, name in entries:
            info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
        info = ZipInfo("spark_runtime_manifest.json", date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = ZIP_DEFLATED
        archive.writestr(
            info, json.dumps(manifest, sort_keys=True, separators=(",", ":"))
        )
    manifest["sha256"] = hashlib.sha256(output.read_bytes()).hexdigest()
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("build/nyc_spark_jobs.zip"))
    parser.add_argument(
        "--check", action="store_true", help="Build and validate the artifact contract."
    )
    args = parser.parse_args()
    manifest = build(args.output)
    if args.check:
        with ZipFile(args.output) as archive:
            names = set(archive.namelist())
        missing = [entry for entry in manifest["entrypoints"] if entry not in names]
        if missing or "spark_runtime_manifest.json" not in names:
            raise SystemExit(f"Spark package contract failed; missing={missing}")
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
