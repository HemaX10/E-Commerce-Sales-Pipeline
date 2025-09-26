{{
    config(
        materialized = 'table'
    )
}}

WITH unique_product 
AS (
    SELECT Product_code , 
           Product_description , 
           Unit_price ,
           ROW_NUMBER() OVER(PARTITION BY Product_code ORDER BY invoice_date) AS RN
    FROM {{ ref('stg_transactions')}}
)

SELECT  {{ dbt_utils.generate_surrogate_key(['Product_code' , 'Product_description' , 'Unit_price'])}} AS Product_key , 
        Product_code , 
        Product_description ,
        Unit_price AS Product_price 
FROM unique_product
WHERE RN = 1