WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Inventory') }}
)

SELECT
    inventory_id,
    product_id,
    quantity_available,
    reorder_level,
    last_restocked,
    warehouse_location
FROM source