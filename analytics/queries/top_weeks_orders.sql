WITH monthly_orders AS (
	SELECT 
		DATE_TRUNC('month', order_date) AS month,
		order_date,
		COUNT(order_id) AS order_count
	FROM analytics.fct_orders
	WHERE order_status = 'completed'
	GROUP BY 1, 2
), weekly_orders AS (
	SELECT
		month,
		CASE
			WHEN EXTRACT(DAY FROM order_date) BETWEEN 1 AND 7 THEN 'Week 1'
			WHEN EXTRACT(DAY FROM order_date) BETWEEN 8 AND 14 THEN 'Week 2'
			WHEN EXTRACT(DAY FROM order_date) BETWEEN 15 AND 21 THEN 'Week 3'
			ELSE 'Week 4+'
		END AS week_period,
		COUNT(order_count) AS weekly_orders
	FROM monthly_orders
	GROUP BY 1, 2
)

SELECT
	TO_CHAR(month, 'YYYY-MM') AS month,
	week_period,
	weekly_orders,
	ROUND(weekly_orders * 100 / SUM(weekly_orders) OVER(PARTITION BY month), 2) AS percentage_of_month
FROM weekly_orders
ORDER BY week_period;