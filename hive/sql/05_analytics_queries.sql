-- Run through Spark SQL in the ecommerce_analytics catalog after a pipeline run.
SELECT order_status, COUNT(*) AS orders,
       SUM(transaction_value) AS gross_value,
       SUM(CASE WHEN order_status = 'Delivered' THEN transaction_value ELSE 0 END) AS delivered_revenue
FROM ecommerce_analytics.silver_transactions
GROUP BY order_status
ORDER BY order_status;

SELECT category, COUNT(DISTINCT order_id) AS orders,
       SUM(transaction_value) AS gross_value,
       SUM(CASE WHEN order_status = 'Delivered' THEN transaction_value ELSE 0 END) AS delivered_revenue
FROM ecommerce_analytics.silver_transactions
GROUP BY category
ORDER BY delivered_revenue DESC;
