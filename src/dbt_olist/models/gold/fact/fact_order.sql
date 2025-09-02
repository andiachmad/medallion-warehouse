{{ config(materialized='table', schema='gold') }}

WITH order_items AS(
    SELECT
        order_id,
        SUM(price + freight_value) AS total_order_value
    FROM    
        {{ ref('silver_order_items')}}
    GROUP BY
        order_id
),

payments AS(
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value,
        MAX(payment_type) AS payment_method
    FROM 
        {{ ref('silver_order_payments') }}
    GROUP BY 
        order_id
)

SELECT
    d.order_id,
    d.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    d.order_status,
    d.order_purchase_timestamp,
    d.order_approved_at,
    d.order_delivered_customer_date,
    d.order_estimated_delivery_date,
    oi.total_order_value,
    pay.total_payment_value,
    pay.payment_method,
    (d.order_estimated_delivery_date - d.order_delivered_customer_date) AS delivery_delay_days,
    (d.order_purchase_timestamp - d.order_approved_at) AS days_to_approve
FROM
    {{ref ('silver_delivery') }} d
LEFT JOIN
    order_items oi USING (order_id)
LEFT JOIN
    payments pay USING (order_id)
LEFT JOIN
    {{ ref ('silver_customer') }} c USING (customer_id)