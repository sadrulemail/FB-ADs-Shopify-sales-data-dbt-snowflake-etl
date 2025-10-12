{{
    config(
        materialized='table'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_campaigns') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} AS campaign_key,
    campaign_id,
    campaign_name,
    objective,
    status,
    budget_type,
    daily_budget,
    lifetime_budget,
    start_date,
    end_date,
    created_at AS effective_date,
    NULL AS expiration_date,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM source