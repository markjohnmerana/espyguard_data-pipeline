-- ─────────────────────────────────────────────
-- silver_networks
--
-- Source: bronze_networks (raw AP records)
-- Output: one clean row per BSSID per scan
--
-- Transformations applied:
--   1. Deduplicate by (scan_id, bssid)
--   2. Normalize bssid and encryption to uppercase
--   3. Filter corrupt RSSI readings
--   4. Flag open networks for Gold anomaly detection
-- ─────────────────────────────────────────────

WITH deduplicated AS (
    SELECT
        id,
        scan_id,

        -- Preserve null SSIDs (hidden networks are valid)
        -- Trim whitespace from visible SSIDs
        NULLIF(TRIM(ssid), '')     AS ssid,

        -- Normalize to uppercase for consistent joining
        -- AA:bb:cc:DD:EE:FF → AA:BB:CC:DD:EE:FF
        UPPER(TRIM(bssid))         AS bssid,

        rssi,
        channel,
        UPPER(TRIM(encryption))    AS encryption,
        received_at,

        -- Deduplication window:
        -- If same BSSID appears twice in one scan,
        -- rank them by RSSI descending.
        -- row_num=1 = strongest signal = keep this one
        ROW_NUMBER() OVER (
            PARTITION BY scan_id, bssid
            ORDER BY rssi DESC
        ) AS row_num

    FROM {{ source('public', 'bronze_networks') }}

    WHERE bssid IS NOT NULL
      AND bssid != ''

      -- Filter corrupt RSSI:
      -- Valid WiFi RSSI is always negative (-120 to 0 dBm)
      -- Positive or zero values are sensor errors
      AND rssi BETWEEN -120 AND 0
),

cleaned AS (
    SELECT
        id,
        scan_id,
        ssid,
        bssid,
        rssi,
        channel,
        encryption,
        received_at,

        -- Flag open networks for Gold suspicious detection
        CASE
            WHEN encryption = 'OPEN' THEN TRUE
            ELSE FALSE
        END AS is_open_network

    FROM deduplicated
    WHERE row_num = 1   -- keep only best reading per BSSID per scan
)

SELECT * FROM cleaned