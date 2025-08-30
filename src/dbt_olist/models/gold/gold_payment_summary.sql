{{ config(materialized='table', schema='gold') }}

SELECT
    payment_type AS payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(payment_value), 2) AS total_revenue,
    ROUND(AVG(payment_value), 2) AS avg_payment_value
FROM    
    {{ ref('silver_order_payments') }}
GROUP BY
    payment_type
ORDER BY
    total_orders DESC
