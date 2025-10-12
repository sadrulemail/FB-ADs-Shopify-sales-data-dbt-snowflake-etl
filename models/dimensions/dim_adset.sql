{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_adsets') }}
),

campaign_dim AS (
    SELECT * FROM {{ ref('dim_campaign') }}
    WHERE is_current = TRUE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ad_set_id']) }} AS adset_key,
    s.ad_set_id,
    c.campaign_key,
    s.campaign_id,
    s.ad_set_name,
    s.targeting_age_min,
    s.targeting_age_max,
    s.targeting_gender,
    s.targeting_locations,
    s.targeting_interests,
    s.bid_strategy,
    s.bid_amount,
    s.daily_budget,
    s.start_date,
    s.end_date,
    s.status,
    CURRENT_TIMESTAMP() AS effective_date,
    NULL AS expiration_date,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM source s
JOIN campaign_dim c ON s.campaign_id = c.campaign_id