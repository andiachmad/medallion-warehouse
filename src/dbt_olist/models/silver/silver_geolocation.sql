{{ config(materialized='table', schema='silver') }} 

SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM
    {{ ref('bronze_geolocation')}}