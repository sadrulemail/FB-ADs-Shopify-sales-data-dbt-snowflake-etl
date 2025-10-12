{{
    config(
        materialized='view'
    )
}}

WITH targeting_metrics AS (
    SELECT
        das.adset_key,
        das.ad_set_id,
        das.ad_set_name,
        das.targeting_age_min,
        das.targeting_age_max,
        das.targeting_gender,
        das.targeting_locations,
        das.targeting_interests,
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
        das.adset_key,
        das.ad_set_id,
        das.ad_set_name,
        das.targeting_age_min,
        das.targeting_age_max,
        das.targeting_gender,
        das.targeting_locations,
        das.targeting_interests,
        dc.campaign_key,
        dc.campaign_name,
        dc.objective
),

age_range_metrics AS (
    SELECT
        CASE
            WHEN targeting_age_min < 18 AND targeting_age_max < 25 THEN 'Under 25'
            WHEN targeting_age_min < 25 AND targeting_age_max < 35 THEN '25-34'
            WHEN targeting_age_min < 35 AND targeting_age_max < 45 THEN '35-44'
            WHEN targeting_age_min < 45 AND targeting_age_max < 55 THEN '45-54'
            WHEN targeting_age_min >= 55 THEN '55+'
            ELSE 'All Ages'
        END AS age_range,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(conversions) AS conversions,
        SUM(conversion_value) AS conversion_value,
        SUM(reach) AS reach
    FROM targeting_metrics
    GROUP BY 
        CASE
            WHEN targeting_age_min < 18 AND targeting_age_max < 25 THEN 'Under 25'
            WHEN targeting_age_min < 25 AND targeting_age_max < 35 THEN '25-34'
            WHEN targeting_age_min < 35 AND targeting_age_max < 45 THEN '35-44'
            WHEN targeting_age_min < 45 AND targeting_age_max < 55 THEN '45-54'
            WHEN targeting_age_min >= 55 THEN '55+'
            ELSE 'All Ages'
        END
),

gender_metrics AS (
    SELECT
        targeting_gender,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(conversions) AS conversions,
        SUM(conversion_value) AS conversion_value,
        SUM(reach) AS reach
    FROM targeting_metrics
    GROUP BY targeting_gender
),

interest_metrics AS (
    SELECT
        targeting_interests,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(conversions) AS conversions,
        SUM(conversion_value) AS conversion_value,
        SUM(reach) AS reach
    FROM targeting_metrics
    GROUP BY targeting_interests
)

SELECT
    tm.adset_key,
    tm.ad_set_id,
    tm.ad_set_name,
    tm.targeting_age_min,
    tm.targeting_age_max,
    CASE
        WHEN tm.targeting_age_min < 18 AND tm.targeting_age_max < 25 THEN 'Under 25'
        WHEN tm.targeting_age_min < 25 AND tm.targeting_age_max < 35 THEN '25-34'
        WHEN tm.targeting_age_min < 35 AND tm.targeting_age_max < 45 THEN '35-44'
        WHEN tm.targeting_age_min < 45 AND tm.targeting_age_max < 55 THEN '45-54'
        WHEN tm.targeting_age_min >= 55 THEN '55+'
        ELSE 'All Ages'
    END AS age_range,
    tm.targeting_gender,
    tm.targeting_locations,
    tm.targeting_interests,
    tm.campaign_key,
    tm.campaign_name,
    tm.objective,
    tm.impressions,
    tm.clicks,
    tm.spend,
    tm.conversions,
    tm.conversion_value,
    tm.reach,
    -- Calculated metrics
    CASE 
        WHEN tm.impressions > 0 THEN tm.clicks / tm.impressions * 100.0 
        ELSE 0 
    END AS ctr,
    CASE 
        WHEN tm.clicks > 0 THEN tm.spend / tm.clicks 
        ELSE 0 
    END AS cpc,
    CASE 
        WHEN tm.impressions > 0 THEN tm.spend / tm.impressions * 1000 
        ELSE 0 
    END AS cpm,
    CASE 
        WHEN tm.conversions > 0 THEN tm.spend / tm.conversions 
        ELSE 0 
    END AS cost_per_conversion,
    CASE 
        WHEN tm.spend > 0 THEN tm.conversion_value / tm.spend 
        ELSE 0 
    END AS roas,
    -- Benchmark comparisons
    CASE
        WHEN tm.targeting_gender IS NOT NULL THEN
            CASE 
                WHEN gm.spend > 0 AND tm.spend > 0 THEN 
                    (tm.conversion_value / tm.spend) / (gm.conversion_value / gm.spend) * 100
                ELSE 100
            END
        ELSE 100
    END AS gender_roas_index,
    CASE
        WHEN tm.targeting_age_min IS NOT NULL THEN
            CASE 
                WHEN arm.spend > 0 AND tm.spend > 0 THEN 
                    (tm.conversion_value / tm.spend) / (arm.conversion_value / arm.spend) * 100
                ELSE 100
            END
        ELSE 100
    END AS age_roas_index,
    CASE
        WHEN tm.targeting_interests IS NOT NULL THEN
            CASE 
                WHEN im.spend > 0 AND tm.spend > 0 THEN 
                    (tm.conversion_value / tm.spend) / (im.conversion_value / im.spend) * 100
                ELSE 100
            END
        ELSE 100
    END AS interest_roas_index,
    -- Targeting effectiveness score
    CASE
        WHEN tm.spend > 0 THEN
            (
                CASE WHEN tm.impressions > 0 THEN tm.clicks / tm.impressions * 30 ELSE 0 END +
                CASE WHEN tm.clicks > 0 THEN tm.conversions / tm.clicks * 30 ELSE 0 END +
                CASE WHEN tm.spend > 0 THEN tm.conversion_value / tm.spend * 40 ELSE 0 END
            )
        ELSE 0
    END AS targeting_effectiveness_score,
    CURRENT_TIMESTAMP() AS analysis_date
FROM targeting_metrics tm
LEFT JOIN gender_metrics gm ON tm.targeting_gender = gm.targeting_gender
LEFT JOIN age_range_metrics arm ON 
    CASE
        WHEN tm.targeting_age_min < 18 AND tm.targeting_age_max < 25 THEN 'Under 25'
        WHEN tm.targeting_age_min < 25 AND tm.targeting_age_max < 35 THEN '25-34'
        WHEN tm.targeting_age_min < 35 AND tm.targeting_age_max < 45 THEN '35-44'
        WHEN tm.targeting_age_min < 45 AND tm.targeting_age_max < 55 THEN '45-54'
        WHEN tm.targeting_age_min >= 55 THEN '55+'
        ELSE 'All Ages'
    END = arm.age_range
LEFT JOIN interest_metrics im ON tm.targeting_interests = im.targeting_interests
ORDER BY 
    CASE
        WHEN tm.spend > 0 THEN tm.conversion_value / tm.spend
        ELSE 0
    END DESC