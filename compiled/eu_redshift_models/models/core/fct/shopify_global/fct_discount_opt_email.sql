with
-- gathers all customers orders and identify the first order per customer
prep as (
select
	order_enriched.customer_id,
	order_enriched.created_at,
	order_enriched.total_discounts,
	order_enriched.id as order_id,
	lower(order_enriched.email) as email,
	min(order_enriched.created_at) over (partition by order_enriched.customer_id
order by
	order_enriched.created_at rows between unbounded preceding and current row) as min_order_date_per_customer
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched
where
	(order_enriched.financial_status::text = any(array ['paid'::character varying::text,
	'partially_refunded'::character varying::text]))
        ),
-- looking at the first orders and identyfing if discount was used
first_order_with_discount as (
select
	prep.customer_id,
	prep.created_at,
	prep.order_id,
	case
		when prep.total_discounts > 0 then true
		else false
	end as used_discount_1st_order
from
	prep
where
	prep.created_at = prep.min_order_date_per_customer
     ),
     -- looking at klaviyo dim_person table to see if opt_to_email = True
     opt_to_email as 
     (
select
	prep.email,
	prep.customer_id,
	case
		when sum(case when custom_consent = '["email"]' then 1 else 0 end)>0 then true
		else false
	end as opt_to_email
from
	prep
left join klaviyo_global.dim_person on
	prep.email = lower(dim_person.email)
group by
	prep.email,
	prep.customer_id), 
revenue_data as (
    select
processing_view.*, 
order_enriched.gross_revenue, order_enriched.net_revenue_1
from
	"airup_eu_dwh"."dbt_feldm"."fct_shopify_cohort_processing_new" as processing_view
left join "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
on processing_view.customer_id = order_enriched.customer_id and processing_view.order_id = order_enriched.id
),
     summary as (
select
	revenue_data.customer_id,
	first_order_with_discount.order_id as first_order_id,
	first_order_with_discount.created_at::date as first_order_date,
	revenue_data.country,
	revenue_data.region,
	revenue_data.net_revenue_2,
    revenue_data.net_revenue_1,
    revenue_data.gross_revenue,
	revenue_data.nth_order,
	first_order_with_discount.used_discount_1st_order,
	opt_to_email.opt_to_email
from
	revenue_data
left join first_order_with_discount 
     on	revenue_data.customer_id = first_order_with_discount.customer_id
left join opt_to_email 
     on	revenue_data.customer_id = opt_to_email.customer_id
group by
	1,2,3,4,5,6,7,8,9,10,11)
     select
	customer_id,
	first_order_id,
	first_order_date,
	country,
	region,
	used_discount_1st_order,
	opt_to_email,
	sum(net_revenue_2) as total_NR2,
    sum(net_revenue_1) as total_NR1,
    sum(gross_revenue) as total_gross_revenue,
	case
		when sum(case when nth_order >1 then 1 end) >0 then true
		else false
	end as returned_customer
from
	summary
group by
	1,2,3,4,5,6,7