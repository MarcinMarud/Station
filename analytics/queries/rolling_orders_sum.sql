WITH order_sum AS (
	SELECT
		customer_key,
		order_date AS dist_date,
		SUM(total_cost) AS total
	FROM analytics.fct_orders
	GROUP BY 1, 2
)

SELECT
	first_name || ' ' || last_name AS full_name,
	dist_date,
	SUM(total) OVER(PARTITION BY customer_key ORDER BY dist_date) AS rolling_sum_over_orders
FROM analytics.dim_customer
JOIN order_sum
	USING(customer_key);