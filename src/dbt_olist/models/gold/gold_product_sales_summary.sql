{{ config(materialized='table', schema='gold') }}

WITH count_products AS (
    SELECT
        sp.product_category_name AS category_name,
        COUNT(soi.product_id) AS total_items_sold,
        COUNT(DISTINCT soi.order_id) AS total_orders_transaction,
        ROUND(SUM(soi.price), 2) AS total_revenue
    FROM 
        {{ ref('silver_order_items') }} soi
    JOIN 
        {{ ref('silver_products') }} sp ON soi.product_id = sp.product_id
    GROUP BY 
        sp.product_category_name
),

ranked_products AS (
    SELECT
        *,
        ROW_NUMBER() OVER(ORDER BY total_items_sold DESC) AS rank_items,
        RANK() OVER(ORDER BY total_orders_transaction DESC) AS rank_orders,
        RANK() OVER(ORDER BY total_revenue DESC) AS rank_revenue
    FROM count_products
)

SELECT *
FROM ranked_products
