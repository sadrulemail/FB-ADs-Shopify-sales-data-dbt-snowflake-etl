{{ config(
    materialized = 'table',
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="TO_DATE('2018-01-01')",
        end_date="TO_DATE('2028-12-31')"
    )
    }}
),

dates AS (
    SELECT
        DATE_PART(YEAR, date_day) * 10000 + DATE_PART(MONTH, date_day) * 100 + DATE_PART(DAY, date_day) AS date_key,
        date_day AS full_date,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYNAME(date_day) AS day_name,
        DAYOFMONTH(date_day) AS day_of_month,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEKOFYEAR(date_day) AS week_of_year,
        MONTH(date_day) AS month_number,
        MONTHNAME(date_day) AS month_name,
        QUARTER(date_day) AS quarter_number,
        YEAR(date_day) AS year_number,
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,
        NULL AS holiday_name
    FROM date_spine
)

SELECT * FROM dates