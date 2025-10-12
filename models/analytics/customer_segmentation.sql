{{
    config(
        materialized='view'
    )
}}

-- Basic customer information and order metrics
WITH customer_metrics AS (
    SELECT
        dc.customer_key,
        dc.customer_id,
        dc.first_name,
        dc.last_name,
        dc.email,
        dc.city,
        dc.state,
        dc.country,
        COUNT(DISTINCT fos.order_id) AS total_orders,
        SUM(fos.grand_total) AS total_spend,
        AVG(fos.grand_total) AS avg_order_value,
        MIN(dd.full_date) AS first_order_date,
        MAX(dd.full_date) AS last_order_date,
        DATEDIFF('day', MIN(dd.full_date), MAX(dd.full_date)) AS customer_tenure_days,
        DATEDIFF('day', MAX(dd.full_date), CURRENT_DATE()) AS days_since_last_order
    FROM {{ ref('fact_order_summary') }} fos
    JOIN {{ ref('dim_customer') }} dc ON fos.customer_key = dc.customer_key
    JOIN {{ ref('dim_date') }} dd ON fos.order_date_key = dd.date_key
    GROUP BY 
        dc.customer_key, 
        dc.customer_id, 
        dc.first_name, 
        dc.last_name, 
        dc.email, 
        dc.city, 
        dc.state, 
        dc.country
),

-- Calculate purchase frequency
customer_frequency AS (
    SELECT
        customer_key,
        CASE
            WHEN customer_tenure_days = 0 THEN total_orders
            ELSE total_orders / (customer_tenure_days / 30.0)
        END AS monthly_frequency
    FROM customer_metrics
),

-- Find preferred product category
product_preferences AS (
    SELECT
        fos.customer_key,
        dp.category,
        COUNT(*) AS category_count,
        ROW_NUMBER() OVER (PARTITION BY fos.customer_key ORDER BY COUNT(*) DESC) AS category_rank
    FROM {{ ref('fact_sales') }} fs
    JOIN {{ ref('fact_order_summary') }} fos ON fs.order_id = fos.order_id
    JOIN {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
    GROUP BY fos.customer_key, dp.category
)

-- Final customer segmentation
SELECT
    cm.customer_key,
    cm.customer_id,
    cm.first_name,
    cm.last_name,
    cm.email,
    cm.city,
    cm.state,
    cm.country,
    cm.total_orders,
    cm.total_spend,
    cm.avg_order_value,
    cm.first_order_date,
    cm.last_order_date,
    cm.customer_tenure_days,
    cf.monthly_frequency,
    pp.category AS preferred_category,
    
    -- Value segmentation
    CASE
        WHEN cm.total_spend > 1000 AND cf.monthly_frequency > 0.5 THEN 'High Value'
        WHEN cm.total_spend > 500 OR cf.monthly_frequency > 0.3 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_value_segment,
    
    -- Recency segmentation
    CASE
        WHEN cm.days_since_last_order <= 30 THEN 'Active'
        WHEN cm.days_since_last_order <= 90 THEN 'At Risk'
        WHEN cm.days_since_last_order <= 180 THEN 'Lapsed'
        ELSE 'Inactive'
    END AS recency_segment,
    
    CURRENT_TIMESTAMP() AS analysis_date
FROM customer_metrics cm
JOIN customer_frequency cf ON cm.customer_key = cf.customer_key
LEFT JOIN product_preferences pp ON cm.customer_key = pp.customer_key AND pp.category_rank = 1
ORDER BY cm.total_spend DESC