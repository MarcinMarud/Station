WITH top_customers AS (
	SELECT
		*,
		RANK() OVER(ORDER BY total_cost DESC) AS rnk
	FROM analytics.dim_customer
	JOIN analytics.fct_orders
		USING(customer_key)
	WHERE order_status = 'completed'
)

SELECT
	first_name || ' ' || last_name AS full_name,
	total_cost AS total_spend
FROM top_customers
WHERE rnk < 6;