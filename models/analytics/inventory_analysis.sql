{{
    config(
        materialized='view'
    )
}}

WITH inventory_status AS (
    SELECT
        dp.product_key,
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.vendor,
        dw.warehouse_location,
        fi.quantity_available,
        fi.reorder_level,
        fi.last_restocked,
        DATEDIFF('day', fi.last_restocked, CURRENT_DATE()) AS days_since_restock
    FROM {{ ref('fact_inventory') }} fi
    JOIN {{ ref('dim_product') }} dp ON fi.product_key = dp.product_key
    JOIN {{ ref('dim_warehouse') }} dw ON fi.warehouse_key = dw.warehouse_key
    WHERE dp.is_current = TRUE
),

sales_velocity AS (
    SELECT
        dp.product_id,
        SUM(fs.quantity) AS total_quantity_sold,
        COUNT(DISTINCT dd.full_date) AS days_with_sales,
        SUM(fs.quantity) / NULLIF(COUNT(DISTINCT dd.full_date), 0) AS daily_sales_rate
    FROM {{ ref('fact_sales') }} fs
    JOIN {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
    JOIN {{ ref('dim_date') }} dd ON fs.order_date_key = dd.date_key
    WHERE dd.full_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY dp.product_id
)

SELECT
    inv.product_key,
    inv.product_id,
    inv.product_name,
    inv.category,
    inv.vendor,
    inv.warehouse_location,
    inv.quantity_available,
    inv.reorder_level,
    inv.last_restocked,
    inv.days_since_restock,
    sv.total_quantity_sold,
    sv.daily_sales_rate,
    CASE 
        WHEN sv.daily_sales_rate > 0 THEN inv.quantity_available / sv.daily_sales_rate
        ELSE NULL
    END AS days_of_supply,
    CASE
        WHEN inv.quantity_available <= inv.reorder_level THEN 'Reorder'
        WHEN inv.quantity_available <= (inv.reorder_level * 1.5) THEN 'Low Stock'
        WHEN inv.quantity_available > (inv.reorder_level * 5) THEN 'Overstocked'
        ELSE 'Adequate'
    END AS inventory_status,
    CASE
        WHEN sv.daily_sales_rate = 0 THEN 'No Movement'
        WHEN sv.daily_sales_rate > 0 AND inv.quantity_available / sv.daily_sales_rate < 15 THEN 'Fast Moving'
        WHEN sv.daily_sales_rate > 0 AND inv.quantity_available / sv.daily_sales_rate < 45 THEN 'Medium Moving'
        ELSE 'Slow Moving'
    END AS inventory_velocity,
    CURRENT_TIMESTAMP() AS analysis_date
FROM inventory_status inv
LEFT JOIN sales_velocity sv ON inv.product_id = sv.product_id
ORDER BY 
    CASE
        WHEN inv.quantity_available <= inv.reorder_level THEN 1
        WHEN inv.quantity_available <= (inv.reorder_level * 1.5) THEN 2
        ELSE 3
    END,
    inv.category,
    inv.product_name