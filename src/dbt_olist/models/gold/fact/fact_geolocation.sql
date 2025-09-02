{{ config(materialized='table', schema='gold') }}

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        total_order_value,
        delivery_delay_days
    FROM 
        {{ ref('fact_order') }}
),

customers AS (
    SELECT
        customer_id,
        customer_zip_code_prefix AS customer_zip_code
    FROM 
        {{ ref('silver_customer') }}
),

geo AS (
    SELECT *
    FROM 
        {{ ref('silver_geolocation') }}
)

SELECT
    o.order_id,
    c.customer_id,
    g.geolocation_zip_code_prefix,
    g.geolocation_lat,
    g.geolocation_lng,
    g.geolocation_city,
    g.geolocation_state,
    o.total_order_value,
    o.delivery_delay_days AS delivery_delay_days,
    1 AS total_orders
FROM 
    orders o
LEFT JOIN 
    customers c ON o.customer_id = c.customer_id
LEFT JOIN 
    geo g ON c.customer_zip_code = g.geolocation_zip_code_prefix
