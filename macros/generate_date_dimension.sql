{% macro generate_date_dimension(start_date_var, end_date_var) %}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="SELECT " ~ start_date_var ~ " FROM date_range",
        end_date="SELECT " ~ end_date_var ~ " FROM date_range"
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
        FALSE AS is_holiday,  -- This would need custom logic for holidays
        NULL AS holiday_name
    FROM date_spine
)

SELECT * FROM dates

{% endmacro %}