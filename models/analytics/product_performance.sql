{{ config(
    materialized = 'view'
) }}

WITH sales_data AS (
    SELECT 
        dp.product_id,
        dp.product_name,
        dp.category,
        dp.vendor,
        SUM(fs.quantity) AS total_units_sold,
        SUM(fs.line_item_total) AS total_revenue,
        SUM(fs.line_item_total - (dp.cost * fs.quantity)) AS total_profit,
        COUNT(DISTINCT fs.order_id) AS order_count,
        AVG(fs.unit_price) AS avg_selling_price,
        dp.price AS list_price,
        AVG(fs.unit_price) / dp.price AS avg_discount_ratio
    FROM {{ ref('fact_sales') }} fs
    JOIN {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
    JOIN {{ ref('dim_date') }} dd ON fs.order_date_key = dd.date_key
    GROUP BY dp.product_id, dp.product_name, dp.category, dp.vendor, dp.price
),

inventory_data AS (
    SELECT 
        dp.product_id,
        SUM(fi.quantity_available) AS total_inventory,
        AVG(fi.reorder_level) AS avg_reorder_level
    FROM {{ ref('fact_inventory') }} fi
    JOIN {{ ref('dim_product') }} dp ON fi.product_key = dp.product_key
    GROUP BY dp.product_id
)

SELECT
    sd.product_id,
    sd.product_name,
    sd.category,
    sd.vendor,
    sd.total_units_sold,
    sd.total_revenue,
    sd.total_profit,
    sd.total_profit / NULLIF(sd.total_revenue, 0) AS profit_margin,
    sd.order_count,
    sd.avg_selling_price,
    sd.list_price,
    sd.avg_discount_ratio,
    id.total_inventory,
    id.avg_reorder_level,
    sd.total_units_sold / NULLIF(id.total_inventory, 0) AS inventory_turnover,
    CASE
        WHEN sd.total_revenue > 100000 THEN 'High'
        WHEN sd.total_revenue > 50000 THEN 'Medium'
        ELSE 'Low'
    END AS revenue_tier,
    CASE
        WHEN sd.total_profit / NULLIF(sd.total_revenue, 0) > 0.3 THEN 'High Margin'
        WHEN sd.total_profit / NULLIF(sd.total_revenue, 0) > 0.15 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END AS profit_tier,
    CURRENT_TIMESTAMP() AS analysis_date
FROM sales_data sd
LEFT JOIN inventory_data id ON sd.product_id = id.product_id
ORDER BY sd.total_revenue DESC