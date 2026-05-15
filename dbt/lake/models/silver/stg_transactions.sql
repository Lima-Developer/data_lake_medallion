{{ config(
    materialized='table',
    description='Cleaned and typed transactions from Bronze layer'
)}}

WITH source AS (
    SELECT *
    FROM read_parquet('s3://ingestion/bronze/transactions/**/*.parquet')
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
        CAST(order_date AS DATE) AS order_date,
        LOWER(TRIM("status")) AS "status",
        LOWER(TRIM(payment_method)) AS payment_method,
        CAST(quantity AS DECIMAL(10,2)) * CAST(unit_price AS DECIMAL(10,2)) AS total_amount,
        _ingested_at,
        _batch_id,
    FROM source
    WHERE order_id IS NOT NULL
        AND customer_id IS NOT NULL
)

SELECT * FROM cleaned