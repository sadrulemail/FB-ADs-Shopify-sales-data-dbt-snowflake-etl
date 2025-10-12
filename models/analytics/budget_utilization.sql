{{
    config(
        materialized='table'
    )
}}

WITH daily_spend AS (
    SELECT
        dc.campaign_key,
        dc.campaign_id,
        dc.campaign_name,
        dc.objective,
        dc.budget_type,
        dc.daily_budget,
        dc.lifetime_budget,
        dd.full_date,
        dd.day_of_week,
        dd.day_name,
        dd.month_name,
        SUM(fcs.amount_spent) AS daily_amount_spent,
        SUM(fcs.daily_budget) AS total_daily_budget,
        AVG(fcs.budget_utilization_percentage) AS avg_budget_utilization
    FROM {{ ref('fact_campaign_spend') }} fcs
    JOIN {{ ref('dim_campaign') }} dc ON fcs.campaign_key = dc.campaign_key
    JOIN {{ ref('dim_date') }} dd ON fcs.date_key = dd.date_key
    GROUP BY
        dc.campaign_key,
        dc.campaign_id,
        dc.campaign_name,
        dc.objective,
        dc.budget_type,
        dc.daily_budget,
        dc.lifetime_budget,
        dd.full_date,
        dd.day_of_week,
        dd.day_name,
        dd.month_name
),

weekly_spend AS (
    SELECT
        campaign_key,
        campaign_id,
        campaign_name,
        DATE_TRUNC('week', full_date) AS week_start_date,
        SUM(daily_amount_spent) AS weekly_amount_spent,
        SUM(total_daily_budget) AS weekly_budget,
        AVG(avg_budget_utilization) AS weekly_avg_utilization,
        MIN(avg_budget_utilization) AS min_daily_utilization,
        MAX(avg_budget_utilization) AS max_daily_utilization
    FROM daily_spend
    GROUP BY
        campaign_key,
        campaign_id,
        campaign_name,
        DATE_TRUNC('week', full_date)
),

monthly_spend AS (
    SELECT
        campaign_key,
        campaign_id,
        campaign_name,
        DATE_TRUNC('month', full_date) AS month_start_date,
        SUM(daily_amount_spent) AS monthly_amount_spent,
        SUM(total_daily_budget) AS monthly_budget,
        AVG(avg_budget_utilization) AS monthly_avg_utilization
    FROM daily_spend
    GROUP BY
        campaign_key,
        campaign_id,
        campaign_name,
        DATE_TRUNC('month', full_date)
),

day_of_week_patterns AS (
    SELECT
        campaign_key,
        day_of_week,
        day_name,
        AVG(daily_amount_spent) AS avg_daily_spend,
        AVG(avg_budget_utilization) AS avg_utilization
    FROM daily_spend
    GROUP BY
        campaign_key,
        day_of_week,
        day_name
),

campaign_performance AS (
    SELECT
        dc.campaign_key,
        SUM(fap.conversions) AS total_conversions,
        SUM(fap.conversion_value) AS total_conversion_value,
        SUM(fap.spend) AS total_performance_spend
    FROM {{ ref('fact_ad_performance') }} fap
    JOIN {{ ref('dim_ad') }} da ON fap.ad_key = da.ad_key
    JOIN {{ ref('dim_adset') }} das ON da.adset_key = das.adset_key
    JOIN {{ ref('dim_campaign') }} dc ON das.campaign_key = dc.campaign_key
    GROUP BY dc.campaign_key
)

SELECT
    ds.campaign_key,
    ds.campaign_id,
    ds.campaign_name,
    ds.objective,
    ds.budget_type,
    ds.daily_budget,
    ds.lifetime_budget,
    ds.full_date,
    ds.day_of_week,
    ds.day_name,
    ds.month_name,
    ds.daily_amount_spent,
    ds.total_daily_budget,
    ds.avg_budget_utilization,
    ws.week_start_date,
    ws.weekly_amount_spent,
    ws.weekly_budget,
    ws.weekly_avg_utilization,
    ws.min_daily_utilization,
    ws.max_daily_utilization,
    ms.month_start_date,
    ms.monthly_amount_spent,
    ms.monthly_budget,
    ms.monthly_avg_utilization,
    -- Day of week comparison
    dow.avg_daily_spend AS avg_spend_for_day_of_week,
    dow.avg_utilization AS avg_utilization_for_day_of_week,
    -- Performance metrics
    cp.total_conversions,
    cp.total_conversion_value,
    -- Calculated metrics
    CASE 
        WHEN ds.daily_amount_spent > 0 THEN cp.total_conversions / ds.daily_amount_spent 
        ELSE 0 
    END AS conversions_per_spend_dollar,
    CASE 
        WHEN ds.daily_amount_spent > 0 THEN cp.total_conversion_value / ds.daily_amount_spent 
        ELSE 0 
    END AS value_per_spend_dollar,
    -- Budget efficiency score
    CASE
        WHEN ds.total_daily_budget > 0 THEN
            CASE
                WHEN ds.daily_amount_spent / ds.total_daily_budget > 0.95 THEN 'Optimal'
                WHEN ds.daily_amount_spent / ds.total_daily_budget > 0.85 THEN 'Good'
                WHEN ds.daily_amount_spent / ds.total_daily_budget > 0.70 THEN 'Average'
                ELSE 'Underutilized'
            END
        ELSE 'No Budget'
    END AS budget_efficiency_rating,
    -- Budget recommendation
    CASE
        WHEN ds.daily_amount_spent / NULLIF(ds.total_daily_budget, 0) > 0.95 AND 
             cp.total_conversion_value / NULLIF(ds.daily_amount_spent, 0) > 2 THEN 'Increase Budget'
        WHEN ds.daily_amount_spent / NULLIF(ds.total_daily_budget, 0) < 0.70 THEN 'Decrease Budget'
        WHEN ds.daily_amount_spent / NULLIF(ds.total_daily_budget, 0) > 0.95 AND 
             cp.total_conversion_value / NULLIF(ds.daily_amount_spent, 0) < 1 THEN 'Optimize Campaign'
        ELSE 'Maintain Budget'
    END AS budget_recommendation,
    CURRENT_TIMESTAMP() AS analysis_date
FROM daily_spend ds
LEFT JOIN weekly_spend ws ON 
    ds.campaign_key = ws.campaign_key AND 
    DATE_TRUNC('week', ds.full_date) = ws.week_start_date
LEFT JOIN monthly_spend ms ON 
    ds.campaign_key = ms.campaign_key AND 
    DATE_TRUNC('month', ds.full_date) = ms.month_start_date
LEFT JOIN day_of_week_patterns dow ON 
    ds.campaign_key = dow.campaign_key AND 
    ds.day_of_week = dow.day_of_week
LEFT JOIN campaign_performance cp ON ds.campaign_key = cp.campaign_key
ORDER BY 
    ds.campaign_name,
    ds.full_date