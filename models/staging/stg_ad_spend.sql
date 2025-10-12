{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Ad_Spend') }}
)

SELECT
    spend_id,
    campaign_id,
    ad_set_id,
    date,
    daily_budget,
    amount_spent,
    remaining_budget
FROM source