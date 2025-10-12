{{
    config(
        materialized='incremental',
        unique_key='campaign_spend_key'
    )
}}

WITH spend_data AS (
    SELECT * FROM {{ ref('stg_ad_spend') }}
),

campaign_dim AS (
    SELECT * FROM {{ ref('dim_campaign') }}
    WHERE is_current = TRUE
),

adset_dim AS (
    SELECT * FROM {{ ref('dim_adset') }}
    WHERE is_current = TRUE
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['s.spend_id']) }} AS campaign_spend_key,
    s.spend_id,
    c.campaign_key,
    a.adset_key,
    d.date_key,
    s.campaign_id,
    s.ad_set_id,
    s.date,
    s.daily_budget,
    s.amount_spent,
    s.remaining_budget,
    -- Calculated metrics
    CASE 
        WHEN s.daily_budget > 0 THEN s.amount_spent / s.daily_budget * 100.0 
        ELSE 0 
    END AS budget_utilization_percentage,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM spend_data s
JOIN campaign_dim c ON s.campaign_id = c.campaign_id
LEFT JOIN adset_dim a ON s.ad_set_id = a.ad_set_id
JOIN date_dim d ON s.date = d.full_date

{% if is_incremental() %}
WHERE s.date > (SELECT MAX(date) FROM {{ this }})
{% endif %}