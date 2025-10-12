{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Campaigns') }}
)

SELECT
    campaign_id,
    campaign_name,
    objective,
    status,
    budget_type,
    daily_budget,
    lifetime_budget,
    start_date,
    end_date,
    created_at,
    updated_at
FROM source