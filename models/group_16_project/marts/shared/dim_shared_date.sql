-- models/marts/shared/dim_shared_date.sql
WITH dates AS (
    SELECT DISTINCT
        CAST(created_date AS DATE) AS full_date,
        FORMAT_TIME('%H:%M', CAST(created_date AS TIME)) AS hour_of_day
    FROM {{ ref('stg_nyc_311_vehicle_complaints') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        CAST(crash_date AS DATE) AS full_date,
        FORMAT_TIME('%H:%M', PARSE_TIME('%H:%M', crash_time)) AS hour_of_day
    FROM {{ ref('stg_nyc_vehicle_crashes') }}
    WHERE crash_date IS NOT NULL
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS date_key,
        full_date,
        EXTRACT(YEAR FROM full_date) AS year,
        EXTRACT(MONTH FROM full_date) AS month,
        EXTRACT(DAY FROM full_date) AS day,
        FORMAT_DATE('%A', full_date) AS day_of_week,
        FORMAT_DATE('%B', full_date) AS month_name,
        EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) AS is_weekend,
        hour_of_day,
        CASE
            WHEN hour_of_day < '12:00' THEN 'AM'
            ELSE 'PM'
        END AS time_of_day,
        CASE
            WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season
    FROM dates
)

SELECT * FROM final
