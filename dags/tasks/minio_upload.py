
import os
import json
from datetime import datetime
from dotenv import load_dotenv

import boto3
from botocore.client import Config

load_dotenv()


# ─────────────────────────────────────────────
# Load credentials strictly from environment.
# No fallback defaults — if a variable is missing
# we want to fail immediately with a clear error
# ─────────────────────────────────────────────

def _get_config():

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
"""
if __name__ == "__main__":
    config = _get_config()
    print("Loaded config:", config)
"""
def _get_minio_client(config):
   
    return boto3.client(
        "s3",
        endpoint_url          = f"http://{config['MINIO_ENDPOINT']}",
        aws_access_key_id     = config["MINIO_ACCESS_KEY"],
        aws_secret_access_key = config["MINIO_SECRET_KEY"],
        config                = Config(signature_version="s3v4"),
        region_name           = "us-east-1"
    )
"""
if __name__ == "__main__":
    config = _get_config()
    client = _get_minio_client(config)
    print("MinIO client created:", client)
    print(client.list_buckets())
"""

def _ensure_bucket(client, bucket):
    """
    Creates the bucket if it does not exist.
    Idempotent — safe to call on every run.
    """
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        print(f"[minio] Created bucket: {bucket}")
"""
if __name__ == "__main__":
    config = _get_config()
    client = _get_minio_client(config)
    _ensure_bucket(client, config["MINIO_BUCKET"])
    print("MinIO client created and bucket verified.")
    response = client.list_buckets()
    print("Existing Buckets:", [b['Name'] for b in response['Buckets']])


def force_delete_test_bucket():
    config = _get_config()
    client = _get_minio_client(config)
    bucket_name = config["MINIO_BUCKET"]
    
    try:
        print(f"Attempting to delete bucket: {bucket_name}...")
        client.delete_bucket(Bucket=bucket_name)
        print("Done! Bucket deleted.")
    except client.exceptions.NoSuchBucket:
        print("Bucket doesn't exist")
    except Exception as e:
        print(f"Error: {e}")
        print("\nPossible fix: If you have 'Object Locking' or 'Versioning' enabled, "
              "MinIO might block a simple delete even if it looks empty.")

if __name__ == "__main__":
    force_delete_test_bucket()"""

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

"""
if __name__ == "__main__":
    from types import SimpleNamespace
    mock_data = [
        {"scan_id": 1, "device_id": "abc", "received_at": "2026-03-25T10:00:00"}
    ]
    class MockTI:
        def xcom_pull(self, task_ids, key):
            print(f"[MOCK XCOM] task_ids={task_ids}, key={key}")
            return mock_data
    mock_context = {
        "ti": MockTI()
    }
    upload_to_minio(**mock_context)
"""
