 

with shopify_data_preparation as (
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
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
left join "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" order_shipping_line on
	order_enriched.id = order_shipping_line.order_id
left join "airup_eu_dwh"."shopify_global"."fct_order_shipping_tax_line" order_shipping_tax_line on
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
 -----incrememntal table macro---
    
                where order_date >= (  select case when max_order_date < trunc(current_date - interval '3 day') then trunc(current_date - interval '3 day') else max_order_date end 
									from(
										select country, max(order_date) as max_order_date
										from "airup_eu_dwh"."shopify_global"."fact_shopify_order_level_report_unification"
										where country != 'other'
										group by 1
										order by max(order_date) asc
										limit 1)
							)
    