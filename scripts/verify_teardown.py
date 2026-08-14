"""Read-only checks for bounded teardown; never deletes resources."""

from __future__ import annotations

import argparse
import json
from typing import Any

import boto3


def _absent(error: Exception) -> bool:
    response = getattr(error, "response", {})
    return response.get("Error", {}).get("Code") in {
        "404",
        "EntityNotFound",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchBucket",
        "NoSuchEntity",
        "ResourceNotFoundException",
        "ValidationException",
    }


def verify(
    bucket: str,
    *,
    project_name: str,
    environment: str,
    region: str,
    clients: dict[str, Any] | None = None,
) -> dict[str, object]:
    clients = clients or {
        service: boto3.client(service, region_name=region)
        for service in (
            "emr",
            "glue",
            "iam",
            "mwaa",
            "redshift-serverless",
            "s3",
        )
    }
    prefix = f"{project_name}-{environment}"
    result: dict[str, object] = {}

    try:
        clients["s3"].head_bucket(Bucket=bucket)
        result["canonical_bucket"] = "PRESENT"
    except clients["s3"].exceptions.ClientError as error:
        result["canonical_bucket"] = "ABSENT" if _absent(error) else "UNKNOWN"

    objects = clients["s3"].list_objects_v2(Bucket=bucket, Prefix="tmp/", MaxKeys=1)
    result["temporary_prefix"] = (
        "EMPTY" if objects.get("KeyCount", 0) == 0 else "PRESENT"
    )

    active_clusters = (
        clients["emr"]
        .list_clusters(
            ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
        )
        .get("Clusters", [])
    )
    result["transient_emr_cluster"] = (
        "PRESENT"
        if any(
            cluster.get("Name", "").startswith("nyc-hvfhs-")
            for cluster in active_clusters
        )
        else "ABSENT"
    )

    databases = {}
    for name in ("bronze", "silver", "ops"):
        try:
            clients["glue"].get_database(Name=name)
            databases[name] = "PRESENT"
        except clients["glue"].exceptions.EntityNotFoundException:
            databases[name] = "ABSENT"
    result["canonical_databases"] = databases

    try:
        clients["mwaa"].get_environment(Name=prefix)
        result["mwaa_environment"] = "PRESENT"
    except clients["mwaa"].exceptions.ClientError as error:
        result["mwaa_environment"] = "ABSENT" if _absent(error) else "UNKNOWN"

    try:
        clients["redshift-serverless"].get_workgroup(workgroupName=prefix)
        result["redshift_workgroup"] = "PRESENT"
    except clients["redshift-serverless"].exceptions.ClientError as error:
        result["redshift_workgroup"] = "ABSENT" if _absent(error) else "UNKNOWN"

    for kind, name in (
        ("mwaa_role", f"{prefix}-mwaa"),
        ("emr_service_role", f"{prefix}-emr-service"),
        ("emr_ec2_role", f"{prefix}-emr-ec2"),
        ("redshift_spectrum_role", f"{prefix}-redshift-spectrum"),
    ):
        try:
            clients["iam"].get_role(RoleName=name)
            result[kind] = "PRESENT"
        except clients["iam"].exceptions.NoSuchEntityException:
            result[kind] = "ABSENT"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--project-name", default="nyc-hvfhs-lakehouse")
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    result = verify(
        args.bucket,
        project_name=args.project_name,
        environment=args.environment,
        region=args.region,
    )
    print(json.dumps(result, indent=2, sort_keys=True))

    expected_absent = [
        result["mwaa_environment"],
        result["mwaa_role"],
        result["emr_service_role"],
        result["emr_ec2_role"],
        result["transient_emr_cluster"],
        result["redshift_workgroup"],
        result["redshift_spectrum_role"],
    ]
    if (
        result["canonical_bucket"] != "PRESENT"
        or any(value != "PRESENT" for value in result["canonical_databases"].values())
        or any(value != "ABSENT" for value in expected_absent)
        or result["temporary_prefix"] != "EMPTY"
    ):
        raise SystemExit(
            "Teardown verification did not reach the expected retained-data state."
        )


if __name__ == "__main__":
    main()
