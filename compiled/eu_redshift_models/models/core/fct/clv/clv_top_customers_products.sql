 

with top_50_clv_customers as (
select
	customer_id,
	country_fullname,
	country_grouping,
	predicted_clv,
	recency,
	frequency,
	net_revenue
from
	"airup_eu_dwh"."clv"."predictive_clv_customer_churn" pccc
where
	predicted_clv is not null
order by
	predicted_clv desc
limit 50),
product_bought_by_top50_customers as
(
select
	customer_id,
	category,
	subcategory_1,
	subcategory_2,
	subcategory_3
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line on
	order_enriched.id = order_line.order_id
left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation on
	order_line.sku = shopify_product_categorisation.sku
where
	customer_id in (
	select
		customer_id
	from
		top_50_clv_customers))
select
	*
from
	top_50_clv_customers
join product_bought_by_top50_customers
		using (customer_id)