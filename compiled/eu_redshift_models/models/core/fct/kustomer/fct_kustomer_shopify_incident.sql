---created_by: Nham Dao
---###################################################################################################################
        -- this view contains the ticket information for kustomer schema (it cleans up the data from the original conversation table)
---###################################################################################################################


with date_spine as (
select
	full_date as "date"
from
	"airup_eu_dwh"."reports"."dates"
where
	full_date >= '2022-06-01'
	and full_date <= current_date 
    ),
--     /*#################################################################
--     the following CTEs are creating the neeeded spines for shopify orders
--     and populates them with shopify data from shopify orders
--     #################################################################*/
-- creates a country spine and adds unspecified to accommodate for Null/Unspecified countries in the incidents CTE
order_spine as (
select
	distinct
            order_enriched.country_fullname as country,
			case when country_mapping.country_grouping is not null then country_mapping.country_grouping
			else 'Unspecified' end as country_grouping
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" as order_enriched
left join "airup_eu_dwh"."public"."country_system_account_mapping" country_mapping 
on country_mapping.country_fullname = order_enriched.country_fullname
    ),
-- combines a order_spine spine with date_spine
order_and_date_spine as (
select
	order_spine.country,
	order_spine.country_grouping,
	date_spine.date
from
	order_spine
left join date_spine on
	1 = 1),
-- gathers shopify orders
orders as (
select
	sum(order_enriched.gross_orders) as orders,
	date(date_trunc('day'::text, order_enriched.created_at)) as date,
	order_enriched.country_fullname as country
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
where
	order_enriched.created_at::date >= '2022-06-01'
group by
	2,
	3
            ),
-- populates the order_and_date spine with shopify orders
orders_spined as (
select
	order_and_date_spine.date,
	order_and_date_spine.country,
	order_and_date_spine.country_grouping,
	coalesce(orders.orders, 0) orders
from
	order_and_date_spine
left join orders
    on
	order_and_date_spine.country = orders.country
	and order_and_date_spine.date = orders.date
     ), 
     incidents_based_created_at as (
select
	date(created_at) as created_at,
	status,
	source,
	customer_language,
	contact_reason,
	sub_category1,
	sub_category2,
	sub_category3,
	customer_country,
	country_grouping,
	custom_sales_lead_bool,
	count(*) as tickets, 
	sum(case when status='done' then 1 else 0 end) as resolved_tickets
from
	"airup_eu_dwh"."kustomer"."dim_conversation"
group by
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	11)
	select
	orders_spined.date,
	orders_spined.country,
	orders_spined.country_grouping,
	orders_spined.orders,
	incidents_based_created_at.resolved_tickets,
	incidents_based_created_at.status,
	incidents_based_created_at.source,
	incidents_based_created_at.tickets,
	incidents_based_created_at.customer_language,
	incidents_based_created_at.custom_sales_lead_bool,
	incidents_based_created_at.contact_reason,
	incidents_based_created_at.sub_category1,
	incidents_based_created_at.sub_category2,
	incidents_based_created_at.sub_category3
from
	orders_spined
left join incidents_based_created_at
	on
	orders_spined.date = incidents_based_created_at.created_at
	and orders_spined.country = incidents_based_created_at.customer_country
	and orders_spined.country_grouping = incidents_based_created_at.country_grouping