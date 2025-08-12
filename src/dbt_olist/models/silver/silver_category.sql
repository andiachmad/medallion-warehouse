{{ config(materialized='table', schema='silver')}}

SELECT 
    product_category_name,
    product_category_name_english
FROM
    {{ ref('bronze_category') }}