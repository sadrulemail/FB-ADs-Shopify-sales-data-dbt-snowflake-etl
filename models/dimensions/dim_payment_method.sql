{{ config(
    materialized = 'table',
) }}

WITH payment_methods AS (
    SELECT DISTINCT payment_method FROM {{ ref('stg_orders') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_method']) }} AS payment_method_key,
    payment_method AS payment_method_id,
    payment_method AS payment_method_description,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM payment_methods