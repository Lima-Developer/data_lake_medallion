{{ config(
    materialized='table',
    description='Revenue metrics aggregated by product category'
)}}

WITH completed_orders AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE status = 'completed'
)

SELECT
    category,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_ticket,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
FROM completed_orders
GROUP BY category
ORDER BY total_revenue DESC