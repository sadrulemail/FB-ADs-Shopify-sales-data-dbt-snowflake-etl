{{ config(
    materialized = 'table',
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    -- Use a surrogate key function instead of IDENTITY
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
    country,
    created_at AS effective_date,
    NULL AS expiration_date,
    TRUE AS is_current,
    'RAW_DATA' AS source_system,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM source