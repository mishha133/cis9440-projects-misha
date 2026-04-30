-- models/marts/shared/dim_shared_date.sql
WITH dates AS (
    SELECT DISTINCT
        DATE(created_date) AS full_date
    FROM {{ ref('stg_nyc_311_vehicle_complaints') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        DATE(crash_date) AS full_date
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
        CASE
            WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM full_date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season,
        CASE
            WHEN FORMAT_DATE('%A', full_date) IN ('Saturday', 'Sunday') THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM dates
)

SELECT * FROM final
