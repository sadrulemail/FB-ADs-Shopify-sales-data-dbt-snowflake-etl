{{
    config(
        materialized='table'
    )
}}

WITH campaign_statuses AS (
    SELECT DISTINCT status FROM {{ ref('stg_campaigns') }}
),

adset_statuses AS (
    SELECT DISTINCT status FROM {{ ref('stg_adsets') }}
),

ad_statuses AS (
    SELECT DISTINCT status FROM {{ ref('stg_ads') }}
),

all_statuses AS (
    SELECT status FROM campaign_statuses
    UNION
    SELECT status FROM adset_statuses
    UNION
    SELECT status FROM ad_statuses
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
    status,
    CASE
        WHEN status = 'ACTIVE' THEN 'Currently running'
        WHEN status = 'PAUSED' THEN 'Temporarily paused'
        WHEN status = 'COMPLETED' THEN 'Campaign has ended'
        WHEN status = 'ARCHIVED' THEN 'Archived and not running'
        WHEN status = 'DELETED' THEN 'Deleted from the system'
        ELSE 'Other status'
    END AS status_description,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM all_statuses