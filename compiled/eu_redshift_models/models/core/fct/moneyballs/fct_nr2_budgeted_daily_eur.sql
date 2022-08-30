

SELECT 	
	day_split
	, region
	, country
	, channel
	, CASE 
		WHEN region = 'America' THEN nr2_buget_per_day*COALESCE(dgcr.conversion_rate_eur::double precision, 1.0801) 
		ELSE nr2_buget_per_day
	END AS nr2_buget_per_day
FROM "airup_eu_dwh"."reports"."fct_nr2_budgeted_daily" 
LEFT JOIN "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" dgcr 
ON CASE 
		WHEN region = 'America' THEN 'USD'
		ELSE 'EUR'
	END = dgcr.currency_abbreviation 
AND day_split = creation_date