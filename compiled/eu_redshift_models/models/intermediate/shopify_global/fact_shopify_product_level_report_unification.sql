 

with shopify_data_preparation as (
select
	'shopify'::text as sales_channel,
	order_enriched.customer_id,
	order_enriched.id as order_id,
	date(order_enriched.created_at) as order_date,
	case
		when (order_enriched.created_at - min(order_enriched.created_at) over (partition by order_enriched.customer_id,
		order_enriched.country_fullname)) = '00:00:00'::interval then 'New Customer'
		else 'Returning Customer'
	end as customer_type,
	order_enriched.country_fullname as country,
	order_enriched.country_grouping as region,
	order_enriched.shipping_address_city as city,
	order_enriched.shipping_address_country_code as shipping_country_code,
	coalesce(shopify_product_categorisation.category, 'Outdated/Exhibition Products') as product_category,
	coalesce(shopify_product_categorisation.subcategory_1, 'Outdated/Exhibition Products') as product_subcategory_1,
	coalesce(shopify_product_categorisation.subcategory_2, 'Outdated/Exhibition Products') as product_subcategory_2,
	coalesce(shopify_product_categorisation.subcategory_3, 'Outdated/Exhibition Products') as product_subcategory_3,
	coalesce(shopify_product_categorisation.product_status, 'discontinued'::text) as product_status,
	sum(coalesce(order_line.price, 0) * coalesce(order_line.quantity, 0) - coalesce(discount_allocation.amount, 0)) as gross_revenue,
	sum(coalesce(order_line.price, 0) * (coalesce(order_line.quantity, 0) - coalesce(order_line_refund.quantity, 0)) - coalesce(tax_line.price, 0) - coalesce(discount_allocation.amount, 0)) as line_item_sales_1,
	sum(coalesce(order_line.price, 0) * (coalesce(order_line.quantity, 0) - coalesce(order_line_refund.quantity, 0)) - coalesce(tax_line.price, 0) - coalesce(discount_allocation.amount, 0) * 2) as line_item_sales_2,
	sum(order_line.quantity) as ordered_quantity,
	sum(order_line.quantity - coalesce(order_line_refund.quantity, 0::double precision)) as net_quantity,
	count(distinct order_enriched.id) as orders,
	count(distinct order_enriched.customer_id) as customers
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line on
	order_enriched.id = order_line.order_id
left join "airup_eu_dwh"."shopify_global"."fct_tax_line" tax_line on
	order_line.id = tax_line.order_line_id
left join "airup_eu_dwh"."shopify_global"."fct_discount_allocation" discount_allocation on
	order_line.id = discount_allocation.order_line_id
left join "airup_eu_dwh"."shopify_global"."fct_order_line_refund" order_line_refund on
	order_line.id = order_line_refund.order_line_id
left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation on
	order_line.sku = shopify_product_categorisation.sku
where
	(order_enriched.financial_status in ('paid', 'partially_refunded') and order_enriched.customer_id is not null)
group by
	'shopify'::text,
	order_enriched.customer_id,
	order_enriched.id,
	order_enriched.created_at,
	(date(order_enriched.created_at)),
	order_enriched.country_fullname,
	order_enriched.country_grouping,
	order_enriched.shipping_address_city,
	order_enriched.shipping_address_country_code,
	(coalesce(shopify_product_categorisation.category, 'Outdated/Exhibition Products')),
	(coalesce(shopify_product_categorisation.subcategory_1, 'Outdated/Exhibition Products')),
	(coalesce(shopify_product_categorisation.subcategory_2, 'Outdated/Exhibition Products')),
	(coalesce(shopify_product_categorisation.subcategory_3, 'Outdated/Exhibition Products')),
	(coalesce(shopify_product_categorisation.product_status, 'discontinued')),
    shopify_product_categorisation.product_status
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
	shopify_data_preparation.product_category,
	shopify_data_preparation.product_subcategory_1,
	shopify_data_preparation.product_subcategory_2,
	shopify_data_preparation.product_subcategory_3,
	shopify_data_preparation.product_status,
	sum(shopify_data_preparation.gross_revenue) as gross_revenue,
	sum(shopify_data_preparation.line_item_sales_1) as sales_revenue_1_no_shipping,
	sum(shopify_data_preparation.line_item_sales_2) as sales_revenue_2_no_shipping,
	sum(shopify_data_preparation.ordered_quantity) as ordered_quantity,
	sum(shopify_data_preparation.net_quantity) as net_quantity,
	sum(shopify_data_preparation.orders) as orders,
	sum(shopify_data_preparation.customers) as customers
from
	shopify_data_preparation
group by
	shopify_data_preparation.customer_id,
	shopify_data_preparation.sales_channel,
	shopify_data_preparation.order_date,
	shopify_data_preparation.customer_type,
	shopify_data_preparation.order_id,
	shopify_data_preparation.country,
	shopify_data_preparation.region,
	shopify_data_preparation.city,
	shopify_data_preparation.product_category,
	shopify_data_preparation.product_subcategory_1,
	shopify_data_preparation.product_subcategory_2,
	shopify_data_preparation.product_subcategory_3,
	shopify_data_preparation.product_status
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
	shopify_agg_data.product_category,
	shopify_agg_data.product_subcategory_1,
	shopify_agg_data.product_subcategory_2,
	shopify_agg_data.product_subcategory_3,
	shopify_agg_data.product_status,
	shopify_agg_data.gross_revenue,
	shopify_agg_data.sales_revenue_1_no_shipping,
	shopify_agg_data.sales_revenue_2_no_shipping,
	shopify_agg_data.ordered_quantity,
	shopify_agg_data.net_quantity,
	shopify_agg_data.orders,
	shopify_agg_data.customers
from
	shopify_agg_data

 -----incrememntal table macro---
    
        where order_date >= (  select case when max_order_date < trunc(current_date - interval '3 day') then trunc(current_date - interval '3 day') else max_order_date end 
									from(
										select country, max(order_date) as max_order_date
										from "airup_eu_dwh"."shopify_global"."fact_shopify_product_level_report_unification"
										where country != 'other'
										group by 1
										order by max(order_date) asc
										limit 1)
							)
    