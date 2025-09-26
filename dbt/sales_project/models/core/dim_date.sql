{{ config(
    materialized = 'table'
) }}

WITH unique_date_transaction AS (
    SELECT DISTINCT DATE_TRUNC('day', invoice_date) AS unique_date
    FROM {{ ref('stg_transactions') }}
)

SELECT
    CAST(TO_CHAR(unique_date, 'YYYYMMDD') AS INT) AS Date_key,
    unique_date::date                          AS date,
    DATE_PART('year', unique_date)             AS Year,
    DATE_PART('month', unique_date)            AS Month_number,
    TO_CHAR(unique_date, 'Month')              AS Month_name,
    DATE_PART('day', unique_date)              AS Day_of_month,
    DATE_PART('doy', unique_date)              AS Day_of_year,
    DATE_PART('dow', unique_date)              AS Day_of_week_number,
    TO_CHAR(unique_date, 'Day')                AS Day_of_week_name,
    DATE_PART('quarter', unique_date)          AS Quarter
FROM unique_date_transaction
