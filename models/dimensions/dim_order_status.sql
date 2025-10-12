{{ config(
    materialized = 'table',
) }}

WITH status_values AS (
    SELECT DISTINCT status FROM {{ ref('stg_orders') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
    status AS status_id,
    status AS status_description,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM status_values