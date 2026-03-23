-- ─────────────────────────────────────────────
-- silver_stations
--
-- Source: bronze_stations (raw device records)
-- Output: one clean row per MAC per scan
--
-- Transformations applied:
--   1. Deduplicate by (scan_id, mac)
--   2. Normalize MAC to uppercase
--   3. Filter corrupt RSSI readings
--   4. Filter broadcast MAC (FF:FF:FF:FF:FF:FF)
-- ─────────────────────────────────────────────

WITH deduplicated AS (
    SELECT
        id,
        scan_id,

        -- Normalize MAC address to uppercase
        -- a1:b2:c3:d4:e5:f6 → A1:B2:C3:D4:E5:F6
        UPPER(TRIM(mac))    AS mac,

        rssi,
        channel,
        received_at,

        -- If same MAC appears multiple times in one scan
        -- (common in promiscuous mode — device sends multiple frames)
        -- keep the strongest signal reading
        ROW_NUMBER() OVER (
            PARTITION BY scan_id, mac
            ORDER BY rssi DESC
        ) AS row_num

    FROM {{ source('public', 'bronze_stations') }}

    WHERE mac IS NOT NULL
      AND mac != ''
      AND rssi BETWEEN -120 AND 0
),

cleaned AS (
    SELECT
        id,
        scan_id,
        mac,
        rssi,
        channel,
        received_at

    FROM deduplicated
    WHERE row_num = 1
      -- Remove broadcast MAC — should be filtered
      -- at ESP32 level but this is a safety net
      AND mac != 'FF:FF:FF:FF:FF:FF'
)

SELECT * FROM cleaned