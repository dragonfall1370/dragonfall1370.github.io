


WITH cte AS (
SELECT 
	week,
	country,
	CASE WHEN customer_type = 'active' THEN "share" END AS current_active,
	CASE WHEN customer_type = 'new' THEN LAG("share",1) OVER (PARTITION BY country, customer_type ORDER BY week) END AS previous_new,
	CASE WHEN customer_type IN ('not_activated','at_risk','churned','dormant') THEN LAG("share",1) OVER (PARTITION BY country, customer_type ORDER BY week) END AS previous_lapsed
FROM "airup_eu_dwh"."crm"."dim_retention_cycle_weekly_snapshots"
), cte2 AS (
SELECT 
	week,
	country,
	COALESCE(SUM(current_active),0) AS current_active,
	COALESCE(SUM(previous_new),0) AS previous_new,
	COALESCE(SUM(previous_lapsed),0) AS previous_lapsed,
	SUM(current_active) / SUM(previous_new) AS new_active_conversion,
	SUM(current_active) / SUM(previous_lapsed) AS lapsed_active_conversion
FROM cte
GROUP BY week, country
), cte3 AS (
SELECT 
	week,
	country,
	current_active,
	previous_new,
	previous_lapsed,
	new_active_conversion,
	lapsed_active_conversion,
	LAG(current_active,52) OVER (PARTITION BY country ORDER BY week) AS py_current_active,
	LAG(previous_new,52) OVER (PARTITION BY country ORDER BY week) AS py_previous_new,
	LAG(previous_lapsed,52) OVER (PARTITION BY country ORDER BY week) AS py_previous_lapsed
FROM cte2
GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT 
	week,
	country,
	current_active,
	previous_new,
	previous_lapsed,
	new_active_conversion,
	lapsed_active_conversion,
	CASE WHEN py_current_active > py_previous_new THEN NULL ELSE py_current_active END AS py_current_active,
	CASE WHEN py_previous_new < py_current_active THEN NULL ELSE py_previous_new END AS py_previous_new,
	CASE WHEN py_previous_lapsed < py_current_active THEN NULL ELSE py_previous_lapsed END AS py_previous_lapsed
FROM cte3