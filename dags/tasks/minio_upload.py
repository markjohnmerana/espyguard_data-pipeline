import os
import json
from datetime import datetime

import boto3
from botocore.client import Config


# ─────────────────────────────────────────────
# Load credentials strictly from environment.
# No fallback defaults — if a variable is missing
# we want to fail immediately with a clear error
# ─────────────────────────────────────────────

def _get_config():
    """
    Reads MinIO config from environment variables.
    Raises immediately if any required var is missing.
    Fail fast — better than silently using wrong values.
    """
    required = {
        "MINIO_ENDPOINT":   os.getenv("MINIO_ENDPOINT"),
        "MINIO_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY"),
        "MINIO_SECRET_KEY": os.getenv("MINIO_SECRET_KEY"),
        "MINIO_BUCKET":     os.getenv("MINIO_BUCKET"),
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise EnvironmentError(
            f"[minio] Missing required environment variables: {missing}\n"
            f"Ensure these are set in your .env file."
        )

    return required


def _get_minio_client(config):
    """
    Builds a boto3 S3 client pointed at MinIO.
    Receives config dict — no direct env reads here.
    """
    return boto3.client(
        "s3",
        endpoint_url          = f"http://{config['MINIO_ENDPOINT']}",
        aws_access_key_id     = config["MINIO_ACCESS_KEY"],
        aws_secret_access_key = config["MINIO_SECRET_KEY"],
        config                = Config(signature_version="s3v4"),
        region_name           = "us-east-1"
    )


def _ensure_bucket(client, bucket):
    """
    Creates the bucket if it does not exist.
    Idempotent — safe to call on every run.
    """
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        print(f"[minio] Created bucket: {bucket}")


def upload_to_minio(**context):
    """
    Pulls Bronze data from XCom and uploads to MinIO.

    File path: espyguard-raw/YYYY/MM/DD/HH-MM.json
    """
    # Load and validate config first — fail fast if misconfigured
    config = _get_config()

    data = context["ti"].xcom_pull(
        task_ids="extract_bronze",
        key="bronze_data"
    )

    if not data:
        print("[minio] No data found in XCom. Skipping upload.")
        return

    client = _get_minio_client(config)
    _ensure_bucket(client, config["MINIO_BUCKET"])

    now      = datetime.utcnow()
    filename = f"{now.strftime('%Y/%m/%d/%H-%M')}.json"
    payload  = json.dumps(data, indent=2).encode("utf-8")

    client.put_object(
        Bucket      = config["MINIO_BUCKET"],
        Key         = filename,
        Body        = payload,
        ContentType = "application/json"
    )

    print(f"[minio] Archived {len(data)} scans "
          f"→ {config['MINIO_BUCKET']}/{filename}")


