{{ config(materialized='table', schema='bronze')}}

SELECT *
FROM read_csv_auto('{{ var("data_path") }}/olist_order_reviews_dataset.csv')