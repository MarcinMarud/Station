WITH ranked_customers AS (
	SELECT
		CONCAT(first_name, ' ', last_name) AS customer_name,
		customer_id,
		COUNT(order_id) AS order_count,
		DENSE_RANK() OVER(ORDER BY COUNT(order_id) DESC) AS rnk
	FROM analytics.dim_customer
	JOIN analytics.fct_orders
		USING(customer_key)
	WHERE order_status = 'completed'
		AND order_date >= CURRENT_DATE - INTERVAL '6 months'
	GROUP BY 1, 2
), top_customers AS (
	SELECT *
	FROM ranked_customers
	WHERE rnk < 4	
)

SELECT 
	*,
	CASE
		WHEN rnk = 1 THEN 0.15
		WHEN rnk = 2 THEN 0.10
		ELSE 0.05
	END AS discount_percentage
FROM top_customers;