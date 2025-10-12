{{
    config(
        materialized='incremental',
        unique_key='ad_performance_key'
    )
}}

WITH insights AS (
    SELECT * FROM {{ ref('stg_ad_insights') }}
),

ad_dim AS (
    SELECT * FROM {{ ref('dim_ad') }}
    WHERE is_current = TRUE
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['i.performance_id']) }} AS ad_performance_key,
    i.performance_id,
    a.ad_key,
    a.adset_key,
    d.date_key,
    i.ad_id,
    i.date,
    i.impressions,
    i.clicks,
    i.ctr,
    i.spend,
    i.conversions,
    i.conversion_value,
    i.cost_per_conversion,
    i.reach,
    i.frequency,
    i.unique_clicks,
    -- Calculated metrics
    CASE 
        WHEN i.impressions > 0 THEN i.clicks / i.impressions * 100.0 
        ELSE 0 
    END AS calculated_ctr,
    CASE 
        WHEN i.clicks > 0 THEN i.spend / i.clicks 
        ELSE 0 
    END AS cost_per_click,
    CASE 
        WHEN i.conversions > 0 THEN i.conversion_value / i.conversions 
        ELSE 0 
    END AS avg_conversion_value,
    CASE 
        WHEN i.spend > 0 AND i.conversion_value > 0 THEN i.conversion_value / i.spend 
        ELSE 0 
    END AS roas,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM insights i
JOIN ad_dim a ON i.ad_id = a.ad_id
JOIN date_dim d ON i.date = d.full_date

{% if is_incremental() %}
WHERE i.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}