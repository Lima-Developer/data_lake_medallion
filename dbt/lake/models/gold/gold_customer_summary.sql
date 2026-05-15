{{ config(
    materialized='table',
    description='Customer summary with lifetime metrics'
)}}

WITH orders AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE "status" = 'completed'
),

customer_metrics AS (
    SELECT
        o.customer_id,
        c.customer_name,
        c.city,
        c.state,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_amount) AS lifetime_revenue,
        AVG(o.total_amount) AS avg_order_value,
        MIN(o.order_date) AS first_purchase_date,
        MAX(o.order_date) AS last_purchase_date
    FROM orders o
    LEFT JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.customer_name, c.city, c.state
)

SELECT 
    *,
    last_purchase_date - first_purchase_date AS customer_lifetime_days
FROM customer_metrics
ORDER BY lifetime_revenue DESC