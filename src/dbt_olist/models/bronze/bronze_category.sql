{{ config(materialized='table', schema='bronze')}}

SELECT *
FROM read_csv_auto('{{ var("data_path") }}/product_category_name_translation.csv')