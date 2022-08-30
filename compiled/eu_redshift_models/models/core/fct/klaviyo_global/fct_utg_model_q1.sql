

WITH cte AS (
SELECT
	p.id,
	'2022-01-01'::date AS quarter,
		CASE WHEN p.country = 'Austria' THEN 'Austria'
		WHEN p.country = 'Belgium' THEN 'Belgium'
		WHEN p.country = 'France' THEN 'France'
		WHEN p.country = 'Germany' THEN 'Germany'
		WHEN p.country = 'Italy' THEN 'Italy'
		WHEN p.country = 'Netherlands' THEN 'Netherlands'
		WHEN p.country = 'Poland' THEN 'Poland'
		WHEN p.country = 'Sweden' THEN 'Sweden'
		WHEN p.country = 'Switzerland' THEN 'Switzerland' 
		WHEN p.country = 'United Kingdom' THEN 'United Kingdom'
		ELSE 'other'
	END AS country,
	CASE 
		WHEN LEFT(p.email,1) IN ('h','b','o') THEN 'ucg'
		ELSE 'utg'
	END AS flag,
	SUM(property_value) AS revenue,
	SUM(CASE WHEN e."type" = 'Placed Order' THEN 1 ELSE 0 END) AS orders
FROM "airup_eu_dwh"."klaviyo_global"."dim_event" e
LEFT JOIN "airup_eu_dwh"."klaviyo_global"."dim_person" p
ON p.id = e.person_id 
WHERE e."type" = 'Placed Order'
GROUP BY 1, 2, 3, 4
), cte2 AS (
SELECT
quarter,
country,
flag,
SUM(CASE WHEN flag = 'ucg' THEN 1 ELSE 0 END) AS ucg_audience,
SUM(CASE WHEN flag = 'utg' THEN 1 ELSE 0 END) AS utg_audience,
SUM(CASE WHEN flag = 'ucg' THEN orders ELSE 0 END) AS ucg_orders,
SUM(CASE WHEN flag = 'utg' THEN orders ELSE 0 END) AS utg_orders,
SUM(CASE WHEN flag = 'ucg' THEN revenue ELSE 0 END) AS ucg_revenue,
SUM(CASE WHEN flag = 'utg' THEN revenue ELSE 0 END) AS utg_revenue
FROM cte
GROUP BY 1, 2, 3
) SELECT 
quarter,
country,
flag,
CASE WHEN flag = 'ucg' THEN ucg_audience WHEN flag = 'utg' THEN utg_audience END AS audience,
CASE WHEN flag = 'ucg' THEN ucg_orders WHEN flag = 'utg' THEN utg_orders END AS orders,
CASE WHEN flag = 'ucg' THEN ucg_revenue WHEN flag = 'utg' THEN utg_revenue END AS revenue
FROM cte2