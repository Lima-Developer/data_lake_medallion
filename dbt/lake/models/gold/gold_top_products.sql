{{ config(
    materialized='table',
    description='Top selling products by revenue and quantity'
)}}

WITH completed_orders AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE status = 'completed'
)

SELECT
    product_id,
    product_name,
    category,
    COUNT(DISTINCT order_id) AS times_sold,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_revenue,
    AVG(unit_price) AS avg_unit_price
FROM completed_orders
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC