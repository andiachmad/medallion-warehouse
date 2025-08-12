{{ config(materialized='table', schema='silver')}}

SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS DATE) AS order_purchase_timestamp,
    COALESCE(CAST(order_approved_at AS DATE), DATE '1900-01-01') AS order_approved_at,
    COALESCE(CAST(order_delivered_carrier_date AS DATE), DATE '1900-01-01') AS order_delivered_carrier_date,
    COALESCE(CAST(order_delivered_customer_date AS DATE), DATE '1900-01-01') AS order_delivered_customer_date,
    CAST(order_estimated_delivery_date AS DATE) AS order_estimated_delivery_date
FROM {{ ref('bronze_delivery') }}
