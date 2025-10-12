{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'AdSets') }}
)

SELECT
    ad_set_id,
    campaign_id,
    ad_set_name,
    targeting_age_min,
    targeting_age_max,
    targeting_gender,
    targeting_locations,
    targeting_interests,
    bid_strategy,
    bid_amount,
    daily_budget,
    start_date,
    end_date,
    status
FROM source