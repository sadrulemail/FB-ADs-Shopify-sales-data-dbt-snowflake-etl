{{ config(
    materialized = 'view'
) }}

WITH regional_sales AS (
    SELECT
        COALESCE(fs.shipping_country, dc.country) AS country,
        COALESCE(fs.shipping_state, dc.state) AS state,
        COALESCE(fs.shipping_city, dc.city) AS city,
        dd.year_number,
        dd.quarter_number,
        dd.month_number,
        dd.month_name,
        SUM(fs.line_item_total) AS total_sales,
        COUNT(DISTINCT fs.order_id) AS order_count,
        COUNT(DISTINCT dc.customer_id) AS customer_count,
        SUM(fs.quantity) AS units_sold
    FROM {{ ref('fact_sales') }} fs
    JOIN {{ ref('dim_customer') }} dc ON fs.customer_key = dc.customer_key
    JOIN {{ ref('dim_date') }} dd ON fs.order_date_key = dd.date_key
    GROUP BY 
        COALESCE(fs.shipping_country, dc.country),
        COALESCE(fs.shipping_state, dc.state),
        COALESCE(fs.shipping_city, dc.city),
        dd.year_number,
        dd.quarter_number,
        dd.month_number,
        dd.month_name
),

previous_period AS (
    SELECT
        country,
        state,
        city,
        year_number,
        quarter_number,
        month_number,
        LAG(total_sales) OVER (
            PARTITION BY country, state, city 
            ORDER BY year_number, month_number
        ) AS prev_month_sales,
        LAG(total_sales, 12) OVER (
            PARTITION BY country, state, city 
            ORDER BY year_number, month_number
        ) AS prev_year_sales
    FROM regional_sales
)

SELECT
    rs.country,
    rs.state,
    rs.city,
    rs.year_number,
    rs.quarter_number,
    rs.month_number,
    rs.month_name,
    rs.total_sales,
    rs.order_count,
    rs.customer_count,
    rs.units_sold,
    rs.total_sales / NULLIF(rs.order_count, 0) AS avg_order_value,
    rs.total_sales / NULLIF(rs.customer_count, 0) AS revenue_per_customer,
    pp.prev_month_sales,
    pp.prev_year_sales,
    (rs.total_sales - pp.prev_month_sales) / NULLIF(pp.prev_month_sales, 0) AS month_over_month_growth,
    (rs.total_sales - pp.prev_year_sales) / NULLIF(pp.prev_year_sales, 0) AS year_over_year_growth,
    CURRENT_TIMESTAMP() AS analysis_date
FROM regional_sales rs
LEFT JOIN previous_period pp ON 
    rs.country = pp.country AND
    rs.state = pp.state AND
    rs.city = pp.city AND
    rs.year_number = pp.year_number AND
    rs.quarter_number = pp.quarter_number AND
    rs.month_number = pp.month_number
ORDER BY 
    rs.country,
    rs.state,
    rs.city,
    rs.year_number,
    rs.month_number