---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the discount code used for e-commerce dashboard
---###################################################################################################################

 


WITH global_curr_usd as (
	SELECT 
		date(global_currency_rates.creation_datetime) AS creation_date,
    	global_currency_rates.currency_abbreviation,
    	global_currency_rates.conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" global_currency_rates
    WHERE global_currency_rates.currency_abbreviation::text = 'USD'::text
), discount_application as (
	select da.order_id, da."type", da.code, da.value_type, da.value
	from "airup_eu_dwh"."shopify_global"."dim_discount_application" da
	where type = 'discount_code'
)
	select oe.creation_date, 	
		case when oe.shopify_shop = 'Base' then 'Germany'
			when oe.shopify_shop = 'FR' then 'France'
			when oe.shopify_shop = 'IT' then 'Italy'
			when oe.shopify_shop = 'NL' then 'Netherlands'
			when oe.shopify_shop = 'CH' then 'Switzerland'
			when oe.shopify_shop = 'UK' then 'United Kingdom'
			when oe.shopify_shop = 'SE' then 'Sweden'
			when oe.shopify_shop = 'AT' then 'Austria'
			when oe.shopify_shop = 'US' then 'United States'
			else null
		end as shopify_shop, 
			da.*, oe.total_discounts,
			oe.gross_revenue, 
			oe.gross_revenue * global_curr_usd.conversion_rate_eur AS gross_revenue_usd,
			oe.net_revenue_1, 
			oe.net_revenue_1 * global_curr_usd.conversion_rate_eur AS net_revenue_1_usd, 
			oe.net_revenue_2, 
			oe.net_revenue_2 * global_curr_usd.conversion_rate_eur AS net_revenue_2_usd,
			net_orders, oe.net_volume
	from discount_application da
	left join "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe on da.order_id = oe.id 
	left join global_curr_usd on oe.creation_date = global_curr_usd.creation_date
	where oe.total_discounts is not null
	order by 1 desc