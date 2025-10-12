{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Ad_Insights') }}
)

SELECT
    performance_id,
    ad_id,
    date,
    impressions,
    clicks,
    ctr,
    spend,
    conversions,
    conversion_value,
    cost_per_conversion,
    reach,
    frequency,
    unique_clicks
FROM source