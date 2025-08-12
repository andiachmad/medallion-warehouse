{{ config(materialized='table', schema='silver') }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS DATE) AS shipping_limit_date,
    price,
    freight_value
FROM
    {{ ref('bronze_order_items')}}