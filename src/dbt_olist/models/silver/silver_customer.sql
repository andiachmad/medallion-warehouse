{{ config(materialized='table', schema='silver') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM
    {{ ref('bronze_customer') }}