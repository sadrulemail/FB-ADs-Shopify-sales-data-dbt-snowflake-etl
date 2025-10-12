WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Order_Items') }}
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_item_tax,
    line_item_shipping,
    line_item_discount,
    line_item_total
FROM source