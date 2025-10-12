WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Orders') }}
)

SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    shipping_address,
    shipping_city,
    shipping_state,
    shipping_zip,
    shipping_country,
    payment_method,
    tax_amount,
    shipping_amount,
    discount_amount,
    grand_total
FROM source