{{ config(materialized='table', schema='silver') }}

SELECT
    review_id,
    order_id,
    review_score,
    COALESCE(review_comment_title, 'Unknown') AS review_comment_title,
    COALESCE(review_comment_message, 'Unknown') AS review_comment_message,
    CAST(review_creation_date AS DATE) AS review_creation_date,
    CAST(review_answer_timestamp AS DATE) AS review_answer_timestamp
FROM    
    {{ ref('bronze_order_review_dataset')}}