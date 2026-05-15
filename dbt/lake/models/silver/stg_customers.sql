{{ config(
    materialized='table',
    description='Cleaned customer data from Bronze Layer'
)}}

WITH source AS (
    SELECT *
    FROM read_parquet('s3://ingestion/bronze/customers/**/*.parquet')
),

cleaned AS (
    SELECT
        customer_id,
        TRIM("name") AS customer_name,
        LOWER(TRIM(email)) AS email,
        TRIM(city) AS city,
        TRIM("state") AS "state",
        CAST(created_at AS DATE) AS created_at,
        _ingested_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned