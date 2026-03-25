import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
#print("DB URL:", SUPABASE_DB_URL)
# ─────────────────────────────────────────────
# SQL Query — what we extract from Bronze
#
# We JOIN all three bronze tables into one result:
#   bronze_scans      → parent record (device, timestamp)
#   bronze_networks   → child AP records per scan
#   bronze_stations   → child station records per scan
#
# json_agg() collapses multiple child rows into
# a single JSON array per scan — easier to serialize
# and upload to MinIO as one document per scan.
#
# INTERVAL '30 minutes' → only process recent data.
# Airflow runs every 30 min so this always gets
# exactly one pipeline window worth of data.
# ─────────────────────────────────────────────

EXTRACT_QUERY = """
    SELECT
        s.id            AS scan_id,
        s.device_id,
        s.received_at,

        json_agg(DISTINCT jsonb_build_object(
            'ssid',       n.ssid,
            'bssid',      n.bssid,
            'rssi',       n.rssi,
            'channel',    n.channel,
            'encryption', n.encryption
        )) FILTER (WHERE n.id IS NOT NULL) AS networks,

        json_agg(DISTINCT jsonb_build_object(
            'mac',     st.mac,
            'rssi',    st.rssi,
            'channel', st.channel
        )) FILTER (WHERE st.id IS NOT NULL) AS stations

    FROM bronze_scans s
    LEFT JOIN bronze_networks  n  ON n.scan_id  = s.id
    LEFT JOIN bronze_stations  st ON st.scan_id = s.id
    WHERE s.received_at >= NOW() - INTERVAL '30 minutes'
    GROUP BY s.id, s.device_id, s.received_at
    ORDER BY s.received_at DESC
"""


def extract_bronze(**context):
    """
    Reads last 30 minutes of Bronze data from Supabase.
    Pushes serialized result to XCom for downstream tasks.

    Called by: Airflow PythonOperator (task_id=extract_bronze)
    Pushes to:  XCom key='bronze_data'
    """
    if not SUPABASE_DB_URL:
        raise ValueError("SUPABASE_DB_URL is not set in environment.")

    conn = psycopg2.connect(SUPABASE_DB_URL)
    cur  = conn.cursor()

    cur.execute(EXTRACT_QUERY)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    # Serialize rows into a list of dicts
    # received_at is a datetime — convert to ISO string
    # so it can be stored in XCom (must be JSON serializable)
    data = [
        {
            "scan_id":     str(row[0]),
            "device_id":   row[1],
            "received_at": row[2].isoformat(),
            "networks":    row[3] or [],
            "stations":    row[4] or []
        }
        for row in rows
    ]

    print(f"[extract] Extracted {len(data)} scans "
          f"from the last 30 minutes.")

    # Push to XCom — upload_to_minio will pull this
    context["ti"].xcom_push(key="bronze_data", value=data)