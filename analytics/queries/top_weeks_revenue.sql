WITH monthly_revenues AS (
	SELECT
		DATE_TRUNC('month', order_date) AS month,
		order_date,
		SUM(total_cost) AS daily_revenue
	FROM analytics.fct_orders
	WHERE order_status = 'completed'
	GROUP BY 1, 2
), weekly_aggregates AS (
	SELECT
		month,
		CASE 
			WHEN EXTRACT(DAY FROM order_date) BETWEEN 1 AND 7 THEN 'Week 1'
			WHEN EXTRACT(DAY FROM order_date) BETWEEN 8 AND 14 THEN 'Week 2'
			WHEN EXTRACT(DAY FROM order_date) BETWEEN 15 AND 21 THEN 'Week 3'
			ELSE 'Week 4+'
		END AS week_period,
		SUM(daily_revenue) AS weekly_revenue
	FROM monthly_revenues
	GROUP BY 1, 2
)

SELECT 
	TO_CHAR(month, 'YYYY-MM') AS month,
	week_period,
	weekly_revenue,
	ROUND(weekly_revenue * 100 / SUM(weekly_revenue) OVER(PARTITION BY month), 2) AS percentage_of_month
FROM weekly_aggregates
ORDER BY week_period;