{{ config(materialized='table', schema='gold') }}

WITH items AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value
    FROM {{ ref('silver_order_items') }}
),

products AS (
    SELECT
        product_id,
        product_category_name AS category_name
    FROM {{ ref('silver_products') }}
),

payments AS (
    SELECT
        order_id,
        payment_type
    FROM {{ ref('silver_order_payments') }}
)

SELECT
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    p.category_name,
    i.price,
    i.freight_value,
    (i.price + i.freight_value) AS revenue,
    pay.payment_type
FROM 
    items i
LEFT JOIN 
    products p ON i.product_id = p.product_id
LEFT JOIN 
    payments pay ON i.order_id = pay.order_id
