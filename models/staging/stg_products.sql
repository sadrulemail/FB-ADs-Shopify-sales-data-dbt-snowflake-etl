WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Products') }}
)

SELECT
    product_id,
    product_name,
    description,
    category,
    price,
    cost,
    sku,
    created_at,
    updated_at,
    is_active,
    vendor
FROM source