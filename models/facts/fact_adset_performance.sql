{{
    config(
        materialized='incremental',
        unique_key='adset_performance_key'
    )
}}

WITH insights AS (
    SELECT 
        ad_id,
        date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(conversions) AS conversions,
        SUM(conversion_value) AS conversion_value,
        SUM(reach) AS reach,
        SUM(unique_clicks) AS unique_clicks
    FROM {{ ref('stg_ad_insights') }}
    GROUP BY ad_id, date
),

ads AS (
    SELECT * FROM {{ ref('stg_ads') }}
),

adset_dim AS (
    SELECT * FROM {{ ref('dim_adset') }}
    WHERE is_current = TRUE
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['a.ad_set_id', 'i.date']) }} AS adset_performance_key,
    ads.adset_key,
    d.date_key,
    a.ad_set_id,
    i.date,
    SUM(i.impressions) AS impressions,
    SUM(i.clicks) AS clicks,
    CASE 
        WHEN SUM(i.impressions) > 0 THEN SUM(i.clicks) / SUM(i.impressions) * 100.0 
        ELSE 0 
    END AS ctr,
    SUM(i.spend) AS spend,
    SUM(i.conversions) AS conversions,
    SUM(i.conversion_value) AS conversion_value,
    CASE 
        WHEN SUM(i.conversions) > 0 THEN SUM(i.spend) / SUM(i.conversions) 
        ELSE 0 
    END AS cost_per_conversion,
    SUM(i.reach) AS reach,
    CASE 
        WHEN SUM(i.reach) > 0 THEN SUM(i.impressions) / SUM(i.reach) 
        ELSE 0 
    END AS frequency,
    SUM(i.unique_clicks) AS unique_clicks,
    -- Additional metrics
    CASE 
        WHEN SUM(i.clicks) > 0 THEN SUM(i.conversions) / SUM(i.clicks) * 100.0 
        ELSE 0 
    END AS conversion_rate,
    CASE 
        WHEN SUM(i.spend) > 0 THEN SUM(i.conversion_value) / SUM(i.spend) 
        ELSE 0 
    END AS roas,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM insights i
JOIN ads a ON i.ad_id = a.ad_id
JOIN adset_dim ads ON a.ad_set_id = ads.ad_set_id
JOIN date_dim d ON i.date = d.full_date
GROUP BY ads.adset_key, d.date_key, a.ad_set_id, i.date

{% if is_incremental() %}
WHERE i.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}