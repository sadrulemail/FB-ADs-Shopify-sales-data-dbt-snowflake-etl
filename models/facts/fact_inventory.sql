{{ config(
    materialized = 'incremental',
    unique_key = 'inventory_key'
) }}

WITH inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
),

dim_product AS (
    SELECT * FROM {{ ref('dim_product') }}
    WHERE is_current = TRUE
),

dim_warehouse AS (
    SELECT * FROM {{ ref('dim_warehouse') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['i.inventory_id']) }} AS inventory_key,
    i.inventory_id,
    dp.product_key,
    dw.warehouse_key,
    dd.date_key,
    i.quantity_available,
    i.reorder_level,
    i.last_restocked,
    'RAW_DATA' AS source_system,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM inventory i
JOIN dim_product dp ON i.product_id = dp.product_id
JOIN dim_warehouse dw ON i.warehouse_location = dw.warehouse_id
JOIN dim_date dd ON DATE(i.last_restocked) = dd.full_date

{% if is_incremental() %}
WHERE i.last_restocked > (SELECT MAX(last_restocked) FROM {{ this }})
{% endif %}