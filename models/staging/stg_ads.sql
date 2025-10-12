{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Ads') }}
)

SELECT
    ad_id,
    ad_set_id,
    ad_name,
    ad_type,
    creative_primary_text,
    creative_headline,
    creative_description,
    display_url,
    destination_url,
    status,
    created_at,
    updated_at
FROM source