

  create view "airup_eu_dwh"."zzz_long_test"."fct_favourite_five_flavour__dbt_tmp" as (
    with five_pod_fav as (
select
	customer_id,
	order_id,
	country,
	net_revenue_2,
	subcategory_3,
	nth_order,
	created_at,
	quantity,
	max(case when subcategory_3 = 'Favourite Five Pod Pack' then 1 else 0 end) over (partition by order_id) as include_fav_5,
	max(case when category = 'Hardware' then 1 else 0 end) over (partition by order_id) as include_drinking_system,
	count(*) over(partition by order_id) as number_of_distinct_product
from
	dbt_nhamdao.fct_order_product_detail
where
	created_at::date >= '2022-05-01'), 
data_prep as (
select
	customer_id,
	created_at::date as created_at,
	order_id,
	country,
	net_revenue_2,
	subcategory_3,
	nth_order,
	include_drinking_system,
	number_of_distinct_product,
	sum(quantity) as quantity
from
	five_pod_fav
where
	include_fav_5 = 1
group by
	1,2,3,4,5,6,7,8,9)
select
	subcategory_3,
	country,
	created_at,
	count(distinct customer_id) as number_of_customer,
	count(distinct order_id) as number_of_order,
	count(distinct (case when nth_order = 1 then customer_id end)) as first_time_customer,
	sum(quantity) as quantity,
	count(distinct (case when include_drinking_system = 1 then order_id end)) as order_combine_drinking_system_fav_five,
	count(distinct (case when number_of_distinct_product = 1 then order_id end)) as order_only_contain_fav_five
from
	data_prep
group by
	1,2,3
having
	subcategory_3 = 'Favourite Five Pod Pack'
  ) with no schema binding;
