{{ config(
    materialized = 'incremental',
    unique_key = 'sales_key'
) }}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

dim_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
    WHERE is_current = TRUE
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_status AS (
    SELECT * FROM {{ ref('dim_order_status') }}
),

dim_payment AS (
    SELECT * FROM {{ ref('dim_payment_method') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} AS sales_key,
    oi.order_id,
    oi.order_item_id,
    dc.customer_key,
    dp.product_key,
    dd.date_key AS order_date_key,
    ds.status_key,
    dpm.payment_method_key,
    oi.quantity,
    oi.unit_price,
    oi.line_item_tax,
    oi.line_item_shipping,
    oi.line_item_discount,
    oi.line_item_total,
    o.order_date,
    o.shipping_address,
    o.shipping_city,
    o.shipping_state,
    o.shipping_zip,
    o.shipping_country,
    'RAW_DATA' AS source_system,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN dim_customer dc ON o.customer_id = dc.customer_id
JOIN dim_product dp ON oi.product_id = dp.product_id
JOIN dim_date dd ON DATE(o.order_date) = dd.full_date
JOIN dim_status ds ON o.status = ds.status_id
JOIN dim_payment dpm ON o.payment_method = dpm.payment_method_id

{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}