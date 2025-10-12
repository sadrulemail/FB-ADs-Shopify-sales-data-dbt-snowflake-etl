{{
    config(
        materialized='view'
    )
}}

WITH campaign_metrics AS (
    SELECT
        dc.campaign_key,
        dc.campaign_id,
        dc.campaign_name,
        dc.objective,
        dc.status,
        dc.budget_type,
        dc.daily_budget,
        dc.lifetime_budget,
        dc.start_date,
        dc.end_date,
        dd.year_number,
        dd.month_number,
        dd.month_name,
        SUM(fap.impressions) AS impressions,
        SUM(fap.clicks) AS clicks,
        SUM(fap.spend) AS spend,
        SUM(fap.conversions) AS conversions,
        SUM(fap.conversion_value) AS conversion_value,
        SUM(fap.reach) AS reach
    FROM {{ ref('fact_ad_performance') }} fap
    JOIN {{ ref('dim_ad') }} da ON fap.ad_key = da.ad_key
    JOIN {{ ref('dim_adset') }} das ON da.adset_key = das.adset_key
    JOIN {{ ref('dim_campaign') }} dc ON das.campaign_key = dc.campaign_key
    JOIN {{ ref('dim_date') }} dd ON fap.date_key = dd.date_key
    GROUP BY 
        dc.campaign_key,
        dc.campaign_id,
        dc.campaign_name,
        dc.objective,
        dc.status,
        dc.budget_type,
        dc.daily_budget,
        dc.lifetime_budget,
        dc.start_date,
        dc.end_date,
        dd.year_number,
        dd.month_number,
        dd.month_name
),

campaign_spend AS (
    SELECT
        dc.campaign_key,
        dd.year_number,
        dd.month_number,
        SUM(fcs.amount_spent) AS total_spend,
        SUM(fcs.daily_budget) AS total_budget,
        AVG(fcs.budget_utilization_percentage) AS avg_budget_utilization
    FROM {{ ref('fact_campaign_spend') }} fcs
    JOIN {{ ref('dim_campaign') }} dc ON fcs.campaign_key = dc.campaign_key
    JOIN {{ ref('dim_date') }} dd ON fcs.date_key = dd.date_key
    GROUP BY 
        dc.campaign_key,
        dd.year_number,
        dd.month_number
)

SELECT
    cm.campaign_key,
    cm.campaign_id,
    cm.campaign_name,
    cm.objective,
    cm.status,
    cm.budget_type,
    cm.daily_budget,
    cm.lifetime_budget,
    cm.start_date,
    cm.end_date,
    cm.year_number,
    cm.month_number,
    cm.month_name,
    cm.impressions,
    cm.clicks,
    cm.spend,
    cm.conversions,
    cm.conversion_value,
    cm.reach,
    -- Calculated metrics
    CASE 
        WHEN cm.impressions > 0 THEN cm.clicks / cm.impressions * 100.0 
        ELSE 0 
    END AS ctr,
    CASE 
        WHEN cm.clicks > 0 THEN cm.spend / cm.clicks 
        ELSE 0 
    END AS cpc,
    CASE 
        WHEN cm.impressions > 0 THEN cm.spend / cm.impressions * 1000 
        ELSE 0 
    END AS cpm,
    CASE 
        WHEN cm.conversions > 0 THEN cm.spend / cm.conversions 
        ELSE 0 
    END AS cost_per_conversion,
    CASE 
        WHEN cm.spend > 0 THEN cm.conversion_value / cm.spend 
        ELSE 0 
    END AS roas,
    CASE 
        WHEN cm.clicks > 0 THEN cm.conversions / cm.clicks * 100.0 
        ELSE 0 
    END AS conversion_rate,
    cs.total_spend,
    cs.total_budget,
    cs.avg_budget_utilization,
    CURRENT_TIMESTAMP() AS analysis_date
FROM campaign_metrics cm
LEFT JOIN campaign_spend cs ON 
    cm.campaign_key = cs.campaign_key AND
    cm.year_number = cs.year_number AND
    cm.month_number = cs.month_number
ORDER BY 
    cm.year_number,
    cm.month_number,
    cm.spend DESC