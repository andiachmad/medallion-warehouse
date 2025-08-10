{{ config(materialized='table', schema='bronze')}}

SELECT *
FROM read_csv_auto('{{ var("data_path") }}/olist_orders_dataset.csv')