SELECT
	fuel_type,
	SUM(fuel_cost) AS total_cost
FROM analytics.dim_fuel
JOIN analytics.fct_orders
	USING(fuel_key)
WHERE order_status = 'completed'
GROUP BY 1
ORDER BY total_cost DESC;
