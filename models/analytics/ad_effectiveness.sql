{{
    config(
        materialized='view'
    )
}}

WITH ad_metrics AS (
    SELECT
        da.ad_key,
        da.ad_id,
        da.ad_name,
        da.ad_type,
        da.creative_headline,
        da.destination_url,
        das.adset_key,
        das.ad_set_name,
        das.targeting_gender,
        dc.campaign_key,
        dc.campaign_name,
        dc.objective,
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
    GROUP BY 
        da.ad_key,
        da.ad_id,
        da.ad_name,
        da.ad_type,
        da.creative_headline,
        da.destination_url,
        das.adset_key,
        das.ad_set_name,
        das.targeting_gender,
        dc.campaign_key,
        dc.campaign_name,
        dc.objective
)

SELECT
    ad_key,
    ad_id,
    ad_name,
    ad_type,
    creative_headline,
    destination_url,
    adset_key,
    ad_set_name,
    targeting_gender,
    campaign_key,
    campaign_name,
    objective,
    impressions,
    clicks,
    spend,
    conversions,
    conversion_value,
    reach,
    -- Calculated metrics
    CASE 
        WHEN impressions > 0 THEN clicks / impressions * 100.0 
        ELSE 0 
    END AS ctr,
    CASE 
        WHEN clicks > 0 THEN spend / clicks 
        ELSE 0 
    END AS cpc,
    CASE 
        WHEN impressions > 0 THEN spend / impressions * 1000 
        ELSE 0 
    END AS cpm,
    CASE 
        WHEN conversions > 0 THEN spend / conversions 
        ELSE 0 
    END AS cost_per_conversion,
    CASE 
        WHEN spend > 0 THEN conversion_value / spend 
        ELSE 0 
    END AS roas,
    CASE 
        WHEN clicks > 0 THEN conversions / clicks * 100.0 
        ELSE 0 
    END AS conversion_rate,
    -- Effectiveness score (custom metric)
    CASE
        WHEN spend > 0 THEN 
            (
                (CASE WHEN impressions > 0 THEN clicks / impressions ELSE 0 END) * 0.2 +
                (CASE WHEN clicks > 0 THEN conversions / clicks ELSE 0 END) * 0.3 +
                (CASE WHEN spend > 0 THEN conversion_value / spend ELSE 0 END) * 0.5
            ) * 100
        ELSE 0
    END AS effectiveness_score,
    -- Effectiveness tier
    CASE
        WHEN spend > 0 THEN 
            CASE
                WHEN (
                    (CASE WHEN impressions > 0 THEN clicks / impressions ELSE 0 END) * 0.2 +
                    (CASE WHEN clicks > 0 THEN conversions / clicks ELSE 0 END) * 0.3 +
                    (CASE WHEN spend > 0 THEN conversion_value / spend ELSE 0 END) * 0.5
                ) * 100 > 50 THEN 'High Performing'
                WHEN (
                    (CASE WHEN impressions > 0 THEN clicks / impressions ELSE 0 END) * 0.2 +
                    (CASE WHEN clicks > 0 THEN conversions / clicks ELSE 0 END) * 0.3 +
                    (CASE WHEN spend > 0 THEN conversion_value / spend ELSE 0 END) * 0.5
                ) * 100 > 25 THEN 'Average'
                ELSE 'Underperforming'
            END
        ELSE 'No Data'
    END AS effectiveness_tier,
    CURRENT_TIMESTAMP() AS analysis_date
FROM ad_metrics
ORDER BY 
    CASE
        WHEN spend > 0 THEN conversion_value / spend
        ELSE 0
    END DESC