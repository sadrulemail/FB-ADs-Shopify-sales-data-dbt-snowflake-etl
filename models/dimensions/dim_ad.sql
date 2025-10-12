{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_ads') }}
),

adset_dim AS (
    SELECT * FROM {{ ref('dim_adset') }}
    WHERE is_current = TRUE
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ad_id']) }} AS ad_key,
    s.ad_id,
    a.adset_key,
    s.ad_set_id,
    s.ad_name,
    s.ad_type,
    s.creative_primary_text,
    s.creative_headline,
    s.creative_description,
    s.display_url,
    s.destination_url,
    s.status,
    s.created_at AS effective_date,
    NULL AS expiration_date,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM source s
JOIN adset_dim a ON s.ad_set_id = a.ad_set_id