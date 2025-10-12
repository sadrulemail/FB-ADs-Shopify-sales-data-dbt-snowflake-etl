WITH source AS (
    SELECT * FROM {{ source('raw_data', 'Customers') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    created_at,
    updated_at,
    address,
    city,
    state,
    zip_code,
    country
FROM source