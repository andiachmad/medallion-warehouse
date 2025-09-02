{{ config(materialized='table', schema='gold')}}

WITH items AS(
    SELECT *
    FROM 
        {{ ref('silver_order_items') }}
),

delivery AS(
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM 
        {{ ref('silver_delivery') }}
),

products AS(
    SELECT
        product_id,
        product_category_name
    FROM
        {{ ref('silver_products') }}
),

sellers AS (
    SELECT
        seller_id,
        seller_city,
        seller_state
    FROM
        {{ ref('silver_sellers') }}
),

payments AS(
    SELECT
        order_id,
        MAX(payment_type) AS payment_method,
        SUM(payment_value) AS total_payment_value,
    FROM
        {{ ref('silver_order_payments')}}
    GROUP BY
        order_id
)

SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    p.product_category_name,
    i.seller_id,
    s.seller_city,
    s.seller_state,
    d.customer_id,
    d.order_status,
    d.order_purchase_timestamp,
    d.order_approved_at,
    d.order_delivered_customer_date,
    d.order_estimated_delivery_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value AS total_item_value,
    pay.payment_method,
    pay.total_payment_value AS total_payment_value,
    (d.order_estimated_delivery_date - d.order_delivered_customer_date) AS delivery_delay_days,
    (d.order_purchase_timestamp - d.order_approved_at) AS days_to_approve
FROM
    items i
LEFT JOIN 
    delivery d USING (order_id)
LEFT JOIN
    products p USING (product_id)
LEFT JOIN
    sellers s USING (seller_id)
LEFT JOIN
    payments pay USING (order_id)