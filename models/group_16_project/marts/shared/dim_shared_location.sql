-- models/marts/shared/dim_shared_location.sql
WITH locations_311 AS (
    SELECT DISTINCT
        borough,
        incident_zip AS zip_code,
        street_name,
        cross_street_1 AS cross_street_name,
        cross_street_2 AS off_street_name,
        latitude,
        longitude
    FROM {{ ref('stg_nyc_311_vehicle_complaints') }}
    WHERE borough IS NOT NULL
),

locations_crashes AS (
    SELECT DISTINCT
        borough,
        zip_code,
        on_street_name AS street_name,
        cross_street_name,
        off_street_name,
        latitude,
        longitude
    FROM {{ ref('stg_nyc_vehicle_crashes') }}
    WHERE borough IS NOT NULL
),

combined AS (
    SELECT * FROM locations_311
    UNION DISTINCT
    SELECT * FROM locations_crashes
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'borough',
            'zip_code',
            'street_name'
        ]) }} AS location_key,
        borough,
        zip_code,
        street_name,
        cross_street_name,
        off_street_name,
        latitude,
        longitude
    FROM combined
)

SELECT * FROM final
