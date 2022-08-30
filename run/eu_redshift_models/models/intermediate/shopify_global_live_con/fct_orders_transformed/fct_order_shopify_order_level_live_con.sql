

  create  table
    "airup_eu_dwh"."shopify_global_live_con"."fct_order_shopify_order_level_live_con__dbt_tmp"
    
    
    
  as (
    ---Authors: Etoma Egot
---Last Modified by: Long VU

---###################################################################################################################

        ---This query adds country mapping information to shopify_global."orders" as well as precalculations for
        ---This is the main enriched order model which combines data from all D2C webshops in a GDPR compliant manner---
        -- This model has been enriched with additionally computed metrics and dimensions such as:
        -- 1. gross revenue : "order".total_price as gross_revenue. Note: total price (includes total discounts)
        -- 2. net revenue 1 : "order".total_price + total discounts - total_tax - return shipments(alias adjustment_amounts)"
        -- 3. net revenue 2 : "order".total_price - total_tax - return shipments(alias adjustment_amounts)"
        -- 4. net volume :  # of Starter Sets/ Pods/ Accessories sold = gross quantity - returned quantity
        -- 5. net orders : only orders which have been paid in full or at least partially are to be counted as net orders
        -- 6: gross orders : every order should be counted as a gross order
        -- 7. shop_country : billing address country
        -- 8. shopify_shop : contains an abbreviation/unique name that describes the webshop where transaction originates
        -- 9: customer_id  : hashed token id of the customer email
        
    ---Note: This model can be used by all users who need enriched order data
---###################################################################################################################

 


with

-- ########################################
-- CALCULATE ORDER ADJUSTMENTS PER ORDER ID
-- ########################################

-- Please note: As of 2021-03-09, there are some open questions around refunds, see https://app.asana.com/0/1160261654509010/1200004660394237/f

order_adjustments_per_order_id as
	(select
		foa.order_id,
		sum(foa.amount) as adjustment_amount
	
    from
        "airup_eu_dwh"."shopify_global_live_con"."fct_order_adjustment_live_con" foa
	group by
		foa.order_id),

-- ########################################
-- CALCULATE THE VOLUME PER ORDER ID
-- ########################################
		
total_quantity_per_order as
	(select
		order_id,
		-- correcting for refunded quantity
		sum(fol.quantity - coalesce(folr.quantity, 0)) as order_total_volume
	from
        "airup_eu_dwh"."shopify_global_live_con"."fct_order_line_live_con" fol

	left join "airup_eu_dwh"."shopify_global_live_con"."fct_order_line_refund_live_con" folr
   
		on fol.id = folr.order_line_id
	group by
		order_id)
-- #######################################################
-- MAIN QUERY
-- #######################################################
,fct_order_enriched_live_con as (
select
	"order".id,
    "order"._fivetran_synced,
    "order".creation_date,
    "order".app_id,
    "order".billing_address_country,
    "order".billing_address_country_code,
    "order".buyer_accepts_marketing,
    "order".cancel_reason,
    "order".cancelled_at,
    "order".checkout_token,
    "order".closed_at,
    "order".confirmed,
    "order".created_at,
    "order".currency,
    "order".shop_cust_id,
    "order".customer_id,
    "order".customer_locale,
    "order".financial_status,
    "order".fulfillment_status,
    "order".landing_site_base_url,
    "order".order_number,
    "order".note,
    "order".number,
    "order".original_order_number,
    "order".payment_gateway_names,
    "order".presentment_currency,
    "order".processed_at,
    "order".processing_method,
    "order".referring_site,
    "order".shipping_address_city,
    "order".shipping_address_country,
    "order".shipping_address_country_code,
    "order".shipping_address_latitude,
    "order".shipping_address_longitude,
    "order".shipping_address_zip,
    "order".source_name,
    "order".subtotal_price,
    "order".subtotal_price_chf,
    "order".subtotal_price_gbp,
    "order".subtotal_price_set,
    "order".taxes_included,
    "order".test,
    "order"."token",
    "order".total_discounts,
    "order".total_discounts_chf,
    "order".total_discounts_gbp,
    "order".total_discounts_set,
    "order".total_line_items_price,
    "order".total_line_items_price_chf,
    "order".total_line_items_price_gbp,
    "order".total_line_items_price_set,
    "order".total_price,
    "order".total_price_chf,
    "order".total_price_gbp,
    "order".total_price_set,
    "order".total_price_usd,
    "order".total_shipping_price_set,
    "order".total_tax,
    "order".total_tax_chf,
    "order".total_tax_gbp,
    "order".total_tax_set,
    "order".total_tip_received,
    "order".total_tip_received_chf,
    "order".total_tip_received_gbp,
    "order".total_weight,
    "order".updated_at,
    "order".user_id,
    "order".shopify_shop,
    "order".currency_abbreviation,
    "order".conversion_rate_eur,

	-- ###########################
	-- COUNTRY MAPPING INFORMATION
	-- ###########################
	
	coalesce(csam.country_fullname, 'other') as country_fullname,
	coalesce(csam.country_abbreviation, 'other') as country_abbreviation,
	coalesce(csam.country_grouping, 'other') as country_grouping,
	
	-- ######################
	-- CALCULATION OF METRICS
	-- ######################
	
	-- GROSS REVENUE
	"order".total_price as gross_revenue,
	
	-- NET REVENUE 1 : Business logic
	-- "Gross rev (incl. shipping revenue) - VAT - return shipments"
	-- Refunds are calculated via order_adjustments_per_order_id
	case
		when "order".financial_status in ('paid', 'partially_refunded') -- we consider only paid and partial refunds when cmputing net revenue 1
		then "order".total_price + coalesce("order".total_discounts) - coalesce("order".total_tax, 0) - coalesce(order_adjustments_per_order_id.adjustment_amount, 0)
	end as net_revenue_1,
	
	-- NET REVENUE 2: Business logic
	-- "Net revenue 1 minus voucher reduction"
	case
		when "order".financial_status in ('paid', 'partially_refunded') -- we consider only paid and partial refunds when cmputing net revenue 2
		then "order".total_price - coalesce("order".total_tax, 0) - coalesce(order_adjustments_per_order_id.adjustment_amount, 0)
	end as net_revenue_2,
	
	-- NET ORDERS
	-- only orders which have been paid in full or at least partially are to be counted as net orders
	case when "order".financial_status in ('paid', 'partially_refunded') then 1 end as net_orders,
	
	-- GROSS ORDERS
	-- every order should be counted as a gross order
	1 as gross_orders,
	
	-- NET VOLUME
	-- # of Starter Sets/ Pods/ Accessories sold = gross quantity - returned quantity
	case when "order".financial_status in ('paid', 'partially_refunded') then total_quantity_per_order.order_total_volume end as net_volume,
	case
            when lower("order".billing_address_country) = 'netherlands' THEN 'NL'
            when lower("order".billing_address_country) = 'belgium' THEN 'BE'
            when lower("order".billing_address_country) = 'france' THEN 'FR'
            when lower("order".billing_address_country) = 'switzerland' THEN 'CH'
            when lower("order".billing_address_country) = 'united kingdom' THEN 'UK'
            when lower("order".billing_address_country) = 'italy' THEN 'IT'
            when lower("order".billing_address_country) = 'austria' THEN 'AT'
            when lower("order".billing_address_country) = 'germany' THEN 'DE'
            when lower("order".billing_address_country) = 'sweden' THEN 'SE'
            else 'others'
        end as shop_country
from
    "airup_eu_dwh"."shopify_global_live_con"."fct_order_live_con" "order"
left join "airup_eu_dwh"."public"."country_system_account_mapping" csam ON "order".shipping_address_country = (csam.shopify_shipping_address_country)
---public.country_system_account_mapping on
	---"order".shipping_address_country::varchar = any(country_system_account_mapping.shopify_shipping_address_country)
left join order_adjustments_per_order_id on
	"order".id = order_adjustments_per_order_id.order_id
left join total_quantity_per_order on
	"order".id = total_quantity_per_order.order_id
),
shopify_data_preparation as (
select
	'shopify'::text as sales_channel,
	order_enriched.customer_id,
	order_enriched.id as order_id,
	date(order_enriched.created_at) as order_date,
	case
		when (order_enriched.created_at - min(order_enriched.created_at) over (partition by order_enriched.customer_id,
		order_enriched.country_fullname)) = '00:00:00'::interval then 'New Customer'::text
		else 'Returning Customer'::text
	end as customer_type,
	order_enriched.country_fullname as country,
	order_enriched.country_grouping as region,
	order_enriched.shipping_address_city as city,
	order_enriched.shipping_address_country_code as shipping_country_code,
	sum(order_enriched.gross_revenue) as gross_revenue,
	sum(order_enriched.net_revenue_1) as net_revenue_1,
	sum(order_enriched.net_revenue_2) as net_revenue_2,
	sum(order_enriched.net_volume) as net_quantity,
	sum(order_shipping_line.price) as gross_shipping_revenue,
	sum(order_shipping_line.price - coalesce(order_shipping_tax_line.price, 0::double precision)) as net_shipping_revenue,
	count(distinct order_enriched.id) as orders,
	count(distinct order_enriched.customer_id) as customers
from
	fct_order_enriched_live_con as order_enriched
left join "airup_eu_dwh"."shopify_global_live_con"."fct_order_shipping_line_live_con" order_shipping_line on
	order_enriched.id = order_shipping_line.order_id
left join "airup_eu_dwh"."shopify_global_live_con"."fct_order_shipping_tax_line_live_con" order_shipping_tax_line on
	order_shipping_line.id = order_shipping_tax_line.order_shipping_line_id
where
	(order_enriched.financial_status in ('paid', 'partially_refunded')
	and order_enriched.customer_id is not null)
group by
	'shopify'::text,
	order_enriched.customer_id,
	order_enriched.id,
	(date(order_enriched.created_at)),
	order_enriched.created_at,
	order_enriched.country_fullname,
	order_enriched.country_grouping,
	order_enriched.shipping_address_city,
	order_enriched.shipping_address_country_code
        ),
gross_qty_cal as (
select
	order_line.order_id,
	sum(order_line.quantity) as ordered_quantity
from
	"airup_eu_dwh"."shopify_global"."fct_order_line" order_line
group by
	order_line.order_id
        ),
shopify_agg_data as (
select
	shopify_data_preparation.customer_id,
	shopify_data_preparation.sales_channel,
	shopify_data_preparation.order_date,
	shopify_data_preparation.customer_type,
	shopify_data_preparation.order_id,
	shopify_data_preparation.country,
	shopify_data_preparation.region,
	shopify_data_preparation.city,
	case
		when sum(gross_qty_cal.ordered_quantity) < sum(shopify_data_preparation.net_quantity) then sum(shopify_data_preparation.gross_revenue) / (sum(shopify_data_preparation.net_quantity) / sum(gross_qty_cal.ordered_quantity))
		else sum(shopify_data_preparation.gross_revenue)
	end as gross_revenue,
	case
		when sum(gross_qty_cal.ordered_quantity) < sum(shopify_data_preparation.net_quantity) then sum(shopify_data_preparation.net_revenue_1) / (sum(shopify_data_preparation.net_quantity) / sum(gross_qty_cal.ordered_quantity))
		else sum(shopify_data_preparation.net_revenue_1)
	end as net_revenue_1,
	case
		when sum(gross_qty_cal.ordered_quantity) < sum(shopify_data_preparation.net_quantity) then sum(shopify_data_preparation.net_revenue_2) / (sum(shopify_data_preparation.net_quantity) / sum(gross_qty_cal.ordered_quantity))
		else sum(shopify_data_preparation.net_revenue_2)
	end as net_revenue_2,
	case
		when sum(gross_qty_cal.ordered_quantity) < sum(shopify_data_preparation.net_quantity) then sum(shopify_data_preparation.net_quantity) / (sum(shopify_data_preparation.net_quantity) / sum(gross_qty_cal.ordered_quantity))
		else sum(shopify_data_preparation.net_quantity)
	end as net_quantity,
	sum(gross_qty_cal.ordered_quantity) as ordered_quantity,
	sum(shopify_data_preparation.gross_shipping_revenue) as gross_shipping_revenue,
	sum(shopify_data_preparation.net_shipping_revenue) as net_shipping_revenue,
	sum(shopify_data_preparation.orders) as orders,
	sum(shopify_data_preparation.customers) as customers
from
	shopify_data_preparation
left join gross_qty_cal
		using (order_id)
group by
	shopify_data_preparation.customer_id,
	shopify_data_preparation.sales_channel,
	shopify_data_preparation.order_date,
	shopify_data_preparation.customer_type,
	shopify_data_preparation.order_id,
	shopify_data_preparation.country,
	shopify_data_preparation.region,
	shopify_data_preparation.city
        )
 select
	shopify_agg_data.customer_id,
	shopify_agg_data.sales_channel,
	shopify_agg_data.order_date,
	shopify_agg_data.customer_type,
	shopify_agg_data.order_id,
	shopify_agg_data.country,
	shopify_agg_data.region,
	shopify_agg_data.city,
	shopify_agg_data.gross_revenue,
	shopify_agg_data.net_revenue_1,
	shopify_agg_data.net_revenue_2,
	shopify_agg_data.net_quantity,
	shopify_agg_data.ordered_quantity,
	shopify_agg_data.gross_shipping_revenue,
	shopify_agg_data.net_shipping_revenue,
	shopify_agg_data.orders,
	shopify_agg_data.customers
from
	shopify_agg_data
  );