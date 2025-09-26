{{
    config(
        materialized = 'table'
    )
}}

WITH transactions 
AS (
    SELECT * 
    FROM {{ ref('stg_transactions') }}
) , 
dim_customer 
AS ( 
    SELECT * 
    FROM {{ ref('dim_customer') }}
) , 
dim_date
AS (
    SELECT *
    FROM {{ ref('dim_date') }}
) ,
dim_product
AS (
    SELECT * 
    FROM {{ ref('dim_product') }}
) , 
dim_time 
AS (
    SELECT * 
    FROM {{ ref('dim_time') }}
)

SELECT  trans.Transaction_ID , 
        trans.Invoice_number ,
        date.Date_key ,
        cust.Customer_key , 
        prod.Product_key ,
        time.Time_key ,
        trans.Quantity ,
        trans.Total_amount , 
        CASE 
            WHEN LEFT(trans.Invoice_number , 1) = 'C' THEN 1
            ELSE 0 
        END AS is_return
FROM transactions trans 
LEFT JOIN dim_customer cust 
ON trans.Customer_ID = cust.Customer_ID
LEFT JOIN dim_date date 
ON DATE_TRUNC('day', trans.invoice_date) = date.Date
LEFT JOIN dim_product prod 
ON trans.Product_code = prod.Product_code
LEFT JOIN dim_time time 
ON TO_CHAR(trans.invoice_date, 'HH24MISS')::int = time.time_key