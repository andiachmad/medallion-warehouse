{{ config(materialized='table', schema='gold') }}

WITH items AS (
    SELECT
        order_id,
        product_id
    FROM {{ ref('silver_order_items') }}
),

product AS (
    SELECT
        product_id,
        product_category_name AS category_name
    FROM {{ ref('silver_products') }}
),

reviews AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title AS comment_title,
        review_comment_message AS comment_message,
        review_creation_date
    FROM {{ ref('silver_order_reviews') }}
)

SELECT
    r.review_id,
    i.order_id,
    i.product_id,
    p.category_name,
    r.review_score,
    r.comment_title,
    r.comment_message,
    r.review_creation_date
FROM 
    items i
LEFT JOIN 
    product p ON i.product_id = p.product_id
LEFT JOIN 
    reviews r ON i.order_id = r.order_id
