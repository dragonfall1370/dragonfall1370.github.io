 

WITH total AS (
	SELECT 
		CASE 
			WHEN country = 'Austria' THEN 'Austria'
			WHEN country = 'Belgium' THEN 'Belgium'
			WHEN country = 'France' THEN 'France'
			WHEN country = 'Germany' THEN 'Germany'
			WHEN country = 'Italy' THEN 'Italy'
			WHEN country = 'Netherlands' THEN 'Netherlands'
			WHEN country = 'Poland' THEN 'Poland'
			WHEN country = 'Sweden' THEN 'Sweden'
			WHEN country = 'Switzerland' THEN 'Switzerland' 
			WHEN country = 'United Kingdom' THEN 'United Kingdom'
			ELSE 'other'
		END AS country
		, CASE WHEN dim_person.custom_first_purchase_date IS NULL THEN 'prospect' ELSE 'customer' END AS customer_status
		, COUNT(id) AS total
	FROM "airup_eu_dwh"."klaviyo_global"."dim_person"
	GROUP BY 
		CASE WHEN dim_person.custom_first_purchase_date IS NULL THEN 'prospect' ELSE 'customer' END
		, CASE 
			WHEN country = 'Austria' THEN 'Austria'
			WHEN country = 'Belgium' THEN 'Belgium'
			WHEN country = 'France' THEN 'France'
			WHEN country = 'Germany' THEN 'Germany'
			WHEN country = 'Italy' THEN 'Italy'
			WHEN country = 'Netherlands' THEN 'Netherlands'
			WHEN country = 'Poland' THEN 'Poland'
			WHEN country = 'Sweden' THEN 'Sweden'
			WHEN country = 'Switzerland' THEN 'Switzerland' 
			WHEN country = 'United Kingdom' THEN 'United Kingdom'
			ELSE 'other'
		END
), suppressed AS (
	SELECT DISTINCT 
		dim_person.email
		, ks.email AS suppressed
		, custom_consent_timestamp
		, custom_consent
		, CASE 
			WHEN dim_person.country = 'Austria' THEN 'Austria'
			WHEN dim_person.country = 'Belgium' THEN 'Belgium'
			WHEN dim_person.country = 'France' THEN 'France'
			WHEN dim_person.country = 'Germany' THEN 'Germany'
			WHEN dim_person.country = 'Italy' THEN 'Italy'
			WHEN dim_person.country = 'Netherlands' THEN 'Netherlands'
			WHEN dim_person.country = 'Poland' THEN 'Poland'
			WHEN dim_person.country = 'Sweden' THEN 'Sweden'
			WHEN dim_person.country = 'Switzerland' THEN 'Switzerland' 
			WHEN dim_person.country = 'United Kingdom' THEN 'United Kingdom'
			ELSE 'other'
		END AS country
		, CASE WHEN dim_person.custom_first_purchase_date IS NULL THEN 'prospect' ELSE 'customer' END AS customer_status
	FROM "airup_eu_dwh"."klaviyo_global"."dim_person" dim_person
	LEFT JOIN "airup_eu_dwh"."dbt_ohigsonspence"."klaviyo_suppressed" ks ON dim_person.email = ks.email AND custom_consent_timestamp < ks."timestamp" 
	WHERE suppressed IS NULL 
), metrics AS (
	SELECT 
	--	date_trunc('day',updated)::date AS updated_at,
		CASE WHEN custom_consent_timestamp IS NOT NULL THEN date_trunc('week',custom_consent_timestamp)::date ELSE '2021-03-01'::date END AS week
		, total.total
		, suppressed.country
		, suppressed.customer_status
		, SUM(CASE WHEN custom_consent LIKE '%email%' THEN 1 ELSE 0 END) AS opt_in
		, SUM(CASE WHEN custom_consent LIKE '%email%' THEN 1 ELSE 0 END) / total.total::double precision AS penetration
	FROM suppressed
	LEFT JOIN total ON total.country = suppressed.country AND total.customer_status = suppressed.customer_status
	GROUP BY CASE WHEN custom_consent_timestamp IS NOT NULL THEN date_trunc('week',custom_consent_timestamp)::date ELSE '2021-03-01'::date END, total, suppressed.country, suppressed.customer_status
)
SELECT 
	week
	, opt_in
	, total
	, penetration
	, country
	, customer_status
	, SUM(opt_in) OVER (PARTITION BY country, total, customer_status ORDER BY week ROWS UNBOUNDED PRECEDING) AS cum_opt_in
	, SUM(penetration) OVER (PARTITION BY country, total, customer_status ORDER BY week ROWS UNBOUNDED PRECEDING) AS cum_penetration
	-- LAG(opt_in,52) OVER (PARTITION BY country ORDER BY week) AS py_opt_in,
	-- LAG(penetration,52) OVER (PARTITION BY country ORDER BY week) AS py_penetration
FROM metrics