{{ config(
    materialized = 'incremental',
    unique_key = 'order_summary_key'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT 
        order_id,
        COUNT(*) AS item_count
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

dim_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
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
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_summary_key,
    o.order_id,
    dc.customer_key,
    dd.date_key AS order_date_key,
    ds.status_key,
    dpm.payment_method_key,
    o.total_amount,
    o.tax_amount,
    o.shipping_amount,
    o.discount_amount,
    o.grand_total,
    oi.item_count AS order_item_count,
    o.order_date,
    'RAW_DATA' AS source_system,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN dim_customer dc ON o.customer_id = dc.customer_id
JOIN dim_date dd ON DATE(o.order_date) = dd.full_date
JOIN dim_status ds ON o.status = ds.status_id
JOIN dim_payment dpm ON o.payment_method = dpm.payment_method_id

{% if is_incremental() %}
WHERE o.order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}