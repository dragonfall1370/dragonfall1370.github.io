

WITH exchange_rates AS (
	select res_currency.name, round(res_currency_rate.rate::double precision, 2) as exchange_rate
	from "airup_eu_dwh"."odoo_currency"."res_currency" res_currency
	join "airup_eu_dwh"."odoo_currency"."res_currency_rate" res_currency_rate
	on res_currency.id = res_currency_rate.currency_id
	where res_currency_rate.create_date::date = getdate()::date
), shop_currency AS (
SELECT 
	flow_id,
	country,
	CASE 
		WHEN country = 'gmbh' THEN 'EUR'
		WHEN country = 'it' THEN 'EUR'
		WHEN country = 'nl' THEN 'EUR'
		WHEN country = 'ch' THEN 'CHF'
		WHEN country = 'fr' THEN 'EUR'
		WHEN country = 'uk' THEN 'GBP'
		WHEN country = 'us' THEN 'USD'
		WHEN country = 'se' THEN 'SEK'
		WHEN country = 'at' THEN 'EUR'
		ELSE NULL 
	END AS currency,
	"date",
	metric_id,
	"_fivetran_batch",
	"_fivetran_index",
	"_fivetran_synced",
	revenue,
	order_count
FROM "airup_eu_dwh"."klaviyo_flow_revenue"."flow_revenue" fr
)
SELECT
flow_id,
country,
currency,
"date",
metric_id,
"_fivetran_batch",
"_fivetran_index",
"_fivetran_synced",
revenue,
revenue/exchange_rate AS eur_revenue,
order_count
FROM shop_currency
LEFT JOIN exchange_rates
ON shop_currency.currency = exchange_rates."name"