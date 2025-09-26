{{
    config(
        materialized = 'table'
    )
}}

WITH unique_times 
AS (
    SELECT DISTINCT TO_CHAR(Invoice_date , 'HH24MISS') AS Time_key ,
    Invoice_date::time AS unique_time
    FROM {{ ref('stg_transactions') }}
)

SELECT  CAST(Time_key AS INT)               AS Time_key , 
        unique_time                         AS Time , 
        DATE_PART('hour' , unique_time)     AS Hour , 
        DATE_PART('minute' , unique_time)   AS Minute , 
        DATE_PART('second' , unique_time)   AS Second , 
        CASE 
            WHEN DATE_PART('hour' , unique_time) BETWEEN 0 AND 5 THEN 'Night'
            WHEN DATE_PART('hour' , unique_time) BETWEEN 5 AND 10 THEN 'Morning'
            WHEN DATE_PART('hour' , unique_time) BETWEEN 10 AND 15 THEN 'Afternoon'
            ELSE 'Evening'
            END                             AS Day_period
FROM unique_times