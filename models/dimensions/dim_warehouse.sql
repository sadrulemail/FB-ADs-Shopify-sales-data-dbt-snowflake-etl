{{ config(
    materialized = 'table',
) }}

WITH warehouse_locations AS (
    SELECT DISTINCT warehouse_location FROM {{ ref('stg_inventory') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['warehouse_location']) }} AS warehouse_key,
    warehouse_location AS warehouse_id,
    warehouse_location,
    warehouse_location AS warehouse_description,
    CURRENT_TIMESTAMP() AS dw_insert_date,
    CURRENT_TIMESTAMP() AS dw_update_date
FROM warehouse_locations