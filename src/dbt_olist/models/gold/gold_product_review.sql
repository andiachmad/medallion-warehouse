{{ config(materialized='table', schema='gold') }}

WITH review AS(
    SELECT
        sp.product_id,
        sp.product_category_name AS category_name,
        sor.review_score,
        sor.review_comment_title
    FROM    
        {{ ref('silver_products')}} sp
    JOIN
        {{ ref('silver_order_items')}} soi ON soi.product_id = sp.product_id
    JOIN
        {{ ref('silver_order_reviews')}} sor ON sor.order_id = soi.order_id
),

aggregated AS(
    SELECT
        product_id,
        category_name,
        ROUND(AVG(review_score), 2) AS avg_review_score,
        COUNT(*) AS total_reviews,
        MODE() WITHIN GROUP (ORDER BY review_comment_title) AS top_review_title
    FROM   
        review
    GROUP BY
        product_id,
        category_name
)

SELECT 
    *
FROM
    aggregated
ORDER BY
    avg_review_score DESC,
    total_reviews DESC
