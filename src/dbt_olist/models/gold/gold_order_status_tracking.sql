{{ config(materialized='table', schema='gold') }}

WITH track AS (
    SELECT
        sd.order_id,
        sd.order_status,
        (sd.order_delivered_customer_date - sd.order_estimated_delivery_date) AS delivery_delay_days,
        (sd.order_approved_at - sd.order_purchase_timestamp) AS days_to_approve,
        soi.price + soi.freight_value AS item_value,
        current_timestamp AS load_timestamp
    FROM
        {{ ref('silver_delivery') }} sd
    JOIN 
        {{ ref('silver_order_items') }} soi ON sd.order_id = soi.order_id
),

agg AS (
    SELECT
        order_id,
        order_status,
        delivery_delay_days,
        days_to_approve,
        SUM(item_value) AS total_order_value,
        load_timestamp,

        -- Hanya delivered/shipped yang punya delivery_delay_days_cleaned, outlier jadi NULL
        CASE
            WHEN order_status NOT IN ('delivered','shipped') THEN NULL
            WHEN delivery_delay_days < -180 OR delivery_delay_days > 365 THEN NULL
            ELSE delivery_delay_days
        END AS delivery_delay_days_cleaned,

        -- Flag outlier untuk delivery_delay_days
        CASE
            WHEN order_status NOT IN ('delivered','shipped') THEN 1
            WHEN delivery_delay_days < -180 OR delivery_delay_days > 365 THEN 1
            ELSE 0
        END AS is_outlier_delivery_delay,

        -- Cleaned days_to_approve sesuai range 0-365
        CASE
            WHEN days_to_approve < 0 OR days_to_approve > 365 THEN NULL
            ELSE days_to_approve
        END AS days_to_approve_cleaned,

        -- Flag outlier days_to_approve
        CASE
            WHEN days_to_approve < 0 OR days_to_approve > 365 THEN 1
            ELSE 0
        END AS is_outlier_days_to_approve

    FROM track
    GROUP BY order_id, order_status, delivery_delay_days, days_to_approve, load_timestamp
)

SELECT * 
FROM agg
