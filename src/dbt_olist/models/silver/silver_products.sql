{{ config(materialized='table', schema='silver')}}

WITH median_values AS(
    SELECT
            --Approx_median adalah fungsi milik DuckDB
        approx_quantile(product_weight_g, 0.5) AS median_weight,
        approx_quantile(product_length_cm, 0.5) AS median_length,
        approx_quantile(product_height_cm, 0.5) AS median_height,
        approx_quantile(product_width_cm, 0.5) AS median_width
    FROM
        {{ ref('bronze_products') }}    
)

SELECT
    product_id,
    COALESCE(product_category_name, 'Unknown') AS product_category_name,
    COALESCE(product_weight_g, median_values.median_weight) AS product_weight_g,
    COALESCE(product_length_cm, median_values.median_length) AS product_length_cm,
    COALESCE(product_height_cm, median_values.median_height) AS product_height_cm,
    COALESCE(product_width_cm, median_values.median_width) AS product_width_cm
FROM {{ ref('bronze_products') }}, median_values
CROSS JOIN median_values
