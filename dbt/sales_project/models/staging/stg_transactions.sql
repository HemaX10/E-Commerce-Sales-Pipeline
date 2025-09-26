{{
    config(
        materialized = 'view'
    )
}}

WITH sales_data 
AS 
(
    SELECT unique_row_id,InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
    FROM {{ source('staging' , 'transactions')}}
)

SELECT 
    unique_row_id AS Transaction_ID ,
    InvoiceNo AS Invoice_number , 
    StockCode AS Product_code , 
    Description AS Product_description , 
    {{ safe_integer_cast('Quantity') }} AS Quantity , 
    CAST(InvoiceDate AS timestamp) AS Invoice_date , 
    CAST(UnitPrice AS numeric) AS Unit_price , 
    CAST(CustomerID AS numeric) AS Customer_ID , 
    UnitPrice * Quantity AS Total_amount ,
    country AS Country 
FROM sales_data

{% if var('is_test_run', default=true) %}

limit 100

{% endif %}
