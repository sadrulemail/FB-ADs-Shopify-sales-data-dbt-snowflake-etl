{{ config(
    materialized = 'table',
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    -- Use a surrogate key function instead of IDENTITY
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    product_name,
    description,
    category,
    price,
    cost,
    sku,
    is_active,
    vendor,
    created_at AS effective_date,
    NULL AS expiration_date,
    TRUE AS is_current,
    'RAW_DATA' AS source_system,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM source