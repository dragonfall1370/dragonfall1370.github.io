---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the order_id, discount code, type and value for E-commerce dashboard
---###################################################################################################################

 

with currency as (
	select ol.shopify_shop,
		case when ol.shopify_shop = 'Base' then 'EUR'
			when ol.shopify_shop = 'FR' then 'EUR'
			when ol.shopify_shop = 'IT' then 'EUR'
			when ol.shopify_shop = 'NL' then 'EUR'
			when ol.shopify_shop = 'CH' then 'CHF'
			when ol.shopify_shop = 'UK' then 'GBP'
			when  ol.shopify_shop = 'SE' then 'SEK'
			when  ol.shopify_shop = 'AT' then 'EUR'
			when  ol.shopify_shop = 'US' then 'USD'
		end as currency	
	from "airup_eu_dwh"."shopify_global"."fct_order_line_w_created_at" ol
	group by 1 
), global_curr_usd as (
	SELECT 
		date(global_currency_rates.creation_datetime) AS creation_date,
    	global_currency_rates.currency_abbreviation,
    	global_currency_rates.conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" global_currency_rates
    WHERE global_currency_rates.currency_abbreviation::text = 'USD'::text
), order_pre as (
	select ol.report_date::date AS creation_date, 
			oe.shop_country,
		case when ol.shopify_shop = 'Base' then 'Germany'
			when ol.shopify_shop = 'FR' then 'France'
			when ol.shopify_shop = 'IT' then 'Italy'
			when ol.shopify_shop = 'NL' then 'Netherlands'
			when ol.shopify_shop = 'CH' then 'Switzerland'
			when ol.shopify_shop = 'UK' then 'United Kingdom'
			when ol.shopify_shop = 'SE' then 'Sweden'
			when ol.shopify_shop = 'AT' then 'Austria'
			when ol.shopify_shop = 'US' then 'United States'			
		end as shopify_shop, 
			currency.currency,
			dgcr.conversion_rate_eur,
			ol.id as order_line_id, 
			oe.id as order_id, 
			oe.customer_id, 
			ol.product_id, 
			ol.sku, 
			spc.category as product_category,
			spc.subcategory as product_subcategory, 
			ol.name as product_name, 
			ol.quantity, 
			olr.quantity as return_quantity,
			ol.price, 
			tl.price as tax, 
			ol.total_discount, 
			olr.subtotal as return_price, 
			olr.total_tax as return_tax,
			(olr.subtotal - olr.total_tax) as net_return_price 
	from "airup_eu_dwh"."shopify_global"."fct_order_line_w_created_at" ol
	left join "airup_eu_dwh"."shopify_global"."fct_tax_line_w_created_at" tl on ol.id = tl.order_line_id and ol.report_date::date = tl.report_date::date
	left join "airup_eu_dwh"."shopify_global"."fct_order_line_refund_w_created_at" olr on ol.id = olr.order_line_id
	left join "airup_eu_dwh"."shopify_global"."dim_ecommerce_product_category" spc on ol.sku = spc.sku
	left join "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe on ol.order_id = oe.id 
    left join currency on currency.shopify_shop = ol.shopify_shop
	Left join "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" dgcr on currency.currency = dgcr.currency_abbreviation and ol.report_date::date = dgcr.creation_date
)
	select order_pre.creation_date, 
			shopify_shop, currency, 
			order_line_id, 
			order_id, 
			customer_id, 
			product_id, 
			sku, 
			product_category, 
			product_subcategory, 
			product_name, 
			quantity, 
			return_quantity, 
			(price * quantity) as gross_revenue_original_currency,
			-- calculate gross revenue and convert all currency to eur
			(price * quantity / order_pre.conversion_rate_eur) as gross_revenue,
			-- convert gross revenue from eur to usd 
		case when shopify_shop = 'United States' then price * quantity
			else (price * quantity / order_pre.conversion_rate_eur * global_curr_usd.conversion_rate_eur) 
		end as gross_revenue_usd,
			-- calculate net revenue 1 and convert all currency to eur 
			(price * quantity + coalesce(total_discount) - coalesce(tax, 0) - coalesce(net_return_price, 0)) / order_pre.conversion_rate_eur as net_reveune_1,
			-- convert net revenue 1 from eur to usd
		case when shopify_shop = 'United States' then (price * quantity + coalesce(total_discount) - coalesce(tax, 0) - coalesce(net_return_price, 0))
			else (price * quantity + coalesce(total_discount) - coalesce(tax, 0) - coalesce(net_return_price, 0)) * order_pre.conversion_rate_eur * global_curr_usd.conversion_rate_eur
		end as net_reveune_1_usd, 
			-- calculate net revenue 2 and convert all currency to eur 
			(price * quantity - coalesce(tax, 0) - coalesce(net_return_price, 0)) / order_pre.conversion_rate_eur as net_revenue_2,
			-- convert net revenue 2 from eur to usd
		case when shopify_shop = 'United States' then (price * quantity - coalesce(tax, 0) - coalesce(net_return_price, 0))
			else (price * quantity - coalesce(tax, 0) - coalesce(net_return_price, 0)) / order_pre.conversion_rate_eur * global_curr_usd.conversion_rate_eur
		end as net_revenue_2_usd
	from order_pre
	left join global_curr_usd on order_pre.creation_date = global_curr_usd.creation_date
	order by order_pre.creation_date desc, order_id, order_line_id