

SELECT 	
	day_split
	, region
	, country
	, channel
	, CASE 
		WHEN region = 'America' THEN media_spend_budget_per_day*COALESCE(dgcr.conversion_rate_eur::double precision, 1.0801) 
		ELSE media_spend_budget_per_day
	END AS media_spend_budget_per_day
FROM "airup_eu_dwh"."reports"."fct_media_spend_budgeted_daily" 
LEFT JOIN "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" dgcr 
ON CASE 
		WHEN region = 'America' THEN 'USD'
		ELSE 'EUR'
	END = dgcr.currency_abbreviation 
AND day_split = creation_date