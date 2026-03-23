-- ─────────────────────────────────────────────
-- gold_network_metrics
--
-- Source: silver_networks
-- Output: one row per BSSID per 30-minute window
--
-- Metrics computed:
--   avg_rssi        → average signal strength
--   min/max_rssi    → signal range in window
--   scan_count      → how many times BSSID was seen
--   is_open_network → no encryption (security risk)
--   is_suspicious   → same SSID from 2+ BSSIDs
--                     (potential evil twin attack)
-- ─────────────────────────────────────────────

WITH windowed AS (
    SELECT
        bssid,
        ssid,
        encryption,
        is_open_network,
        channel,
        rssi,

        -- Round timestamp to nearest 30-minute bucket
        -- 17:43 → 17:30  |  18:07 → 18:00  |  18:31 → 18:30
        --
        -- How it works:
        --   EXTRACT(MINUTE FROM received_at) → gets the minute (43)
        --   FLOOR(43 / 30) → 1 (which 30-min block we're in)
        --   INTERVAL '30 min' * 1 → 30 minutes offset
        --   DATE_TRUNC('hour', ...) → strips minutes/seconds
        --   Add them together → 17:00 + 30min = 17:30
        DATE_TRUNC('hour', received_at) +
            INTERVAL '30 min' * FLOOR(
                EXTRACT(MINUTE FROM received_at) / 30
            ) AS window_start

    FROM {{ ref('silver_networks') }}
    -- ref() instead of source() because this model
    -- depends on silver_networks, not Bronze directly.
    -- dbt uses ref() to build the dependency graph.
),

aggregated AS (
    SELECT
        bssid,
        ssid,
        encryption,
        is_open_network,
        channel,
        window_start,

        ROUND(AVG(rssi), 2) AS avg_rssi,
        MIN(rssi)           AS min_rssi,
        MAX(rssi)           AS max_rssi,
        COUNT(*)            AS scan_count

    FROM windowed
    GROUP BY
        bssid, ssid, encryption,
        is_open_network, channel, window_start
),

-- Evil twin / rogue AP detection:
-- A legitimate network has one BSSID per SSID.
-- If "CoffeeShop_WiFi" appears with 3 different BSSIDs
-- in the same 30-min window, one of them is suspicious.
ssid_bssid_counts AS (
    SELECT
        ssid,
        window_start,
        COUNT(DISTINCT bssid) AS bssid_count

    FROM aggregated
    WHERE ssid IS NOT NULL
    GROUP BY ssid, window_start
)

SELECT
    a.bssid,
    a.ssid,
    a.encryption,
    a.is_open_network,
    a.channel,
    a.window_start,
    a.avg_rssi,
    a.min_rssi,
    a.max_rssi,
    a.scan_count,

    -- is_suspicious = TRUE when same SSID
    -- has 2 or more different BSSIDs in same window
    CASE
        WHEN s.bssid_count > 1 THEN TRUE
        ELSE FALSE
    END AS is_suspicious,

    s.bssid_count AS ssid_bssid_count

FROM aggregated a
LEFT JOIN ssid_bssid_counts s
    ON  a.ssid         = s.ssid
    AND a.window_start = s.window_start