{{
    config(
        materialized = 'table'
    )
}}

WITH unique_customers
AS (
    SELECT DISTINCT (Customer_ID) AS Customer_ID, 
           Country
    FROM {{ ref('stg_transactions') }}
)

SELECT ROW_NUMBER() OVER(ORDER BY Customer_ID , Country) AS Customer_key , 
       Customer_ID , 
       Country 
FROM unique_customers