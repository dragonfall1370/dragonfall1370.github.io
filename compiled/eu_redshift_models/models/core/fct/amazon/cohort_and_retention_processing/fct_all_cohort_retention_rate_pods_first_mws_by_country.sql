--- #### Prep views for the current retention logic (old rebuy rate logic) defined in 2/2022
--- #### Parts of the query carry unclear legacy decisions and it is not optimized for performance
--- #### Created by: Feld M
--- #### Last edited by: Tomas K., Feld M on 1.2.2022



with
    --- ##############################
    --- ### gathers number of SS, pods & accessories in each order
	--- ### adding the new mapping table influence the results (1.2.2022 new/old:SS 1429766/1049917, pods 4067395/4182940, acc 347727/302167)
    --- ##############################
	product_categories_per_order as
		(select
			amazon_order_id as "oid",
			sum(case when product_type = 'Starter Set' then quantity_shipped end) as starter_sets,
			sum(case when product_type = 'Pods' then quantity_shipped end) as pods,
			sum(case when product_type = 'Accessories' then quantity_shipped end) as loops
		from
			"airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched"
		group by
			amazon_order_id),

    --- ##############################
    --- ### CTE doing most of the heavy lifting, e.g., date diff, nth_order, leads
    --- ##############################
	data_prep_by_country as
	(select distinct
		country_abbreviation,
		country_fullname,
		md5(buyer_email) as customer_id,
		amazon_order_id as order_number,
		purchase_date as created_at,
		case
			when (sum(1) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and starter_sets >= 1
			then 1
			else 0
		end as init_order_starter_set,
		case
			when (sum(1) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and pods >= 1
			then 1
			else 0
		end as init_order_pods,
		lead(purchase_date) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc) created_at_next_order,
		lead(purchase_date) over (partition by md5(buyer_email), country_abbreviation order by purchase_date asc) - purchase_date as time_diff_next_order,
		min(date(date_trunc('month', purchase_date))) over (partition by md5(buyer_email), country_abbreviation) as cohort,
		date_trunc('month', purchase_date) as month_created_at,

		-- ##############################
		-- calculate date diff in months
		(DATE_PART('year', purchase_date::date)
		- DATE_PART('year', (min(date_trunc('month', purchase_date)) over (partition by md5(buyer_email), country_abbreviation))::date))
		* 12 +
		(DATE_PART('month', purchase_date::date)
		- DATE_PART('month', (min(date_trunc('month', purchase_date)) over (partition by md5(buyer_email), country_abbreviation))::date)) as date_diff_months,
		-- ##############################

		sum(coalesce(nullif(item_price, 'NaN'), 0) * coalesce(quantity_shipped, 0)) - sum(coalesce(nullif(item_promotion_discount, 'NaN'), 0) * coalesce(quantity_shipped, 0)) as net_revenue,
		starter_sets,
		pods,
		loops,
		pods >= 1 as "pod_order"
	from
		"airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
	left join product_categories_per_order
		on ofse.amazon_order_id = product_categories_per_order."oid"
	group by
		md5(buyer_email),
		country_abbreviation,
		country_fullname,
		amazon_order_id,
		purchase_date,
		starter_sets,
		pods,
		loops
		),


    --- ##############################
    --- ### CTE flagging the whole customers whether init SS / pods
    --- ### and counting orders, pods and pods orders
    --- ##############################
	data_prep_2_by_country as
		(select
			data_prep_by_country.*,
			sum(1) over (partition by customer_id, country_abbreviation order by created_at, order_number asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as nth_order,
			case
				when max(init_order_starter_set) over (partition by customer_id, country_abbreviation) = 1
				then 'yes'
				else 'no'
			end as init_order_incl_starter_set,
			case
				when max(init_order_pods) over (partition by customer_id, country_abbreviation) = 1
				then 'yes'
				else 'no'
			end as init_order_incl_pods,
			sum(pods) over (partition by customer_id, country_abbreviation) as pod_items_per_customer,
			count(pods) over (partition by customer_id, country_abbreviation, date_diff_months) as pod_orders_per_customer
		from
			data_prep_by_country),


	--- ##############################
	--- ### ROLL UP replacement added after migration
	--- ##############################
	main_groupby as
		(select
			cohort,
			country_abbreviation,
			country_fullname,
			date(created_at)::timestamp without time zone as date,
			init_order_incl_starter_set, -- dim 1
			init_order_incl_pods, -- dim 2
			count(distinct customer_id) as customers,
			count(distinct order_number) as orders
		from
			data_prep_2_by_country
		where
			cohort >= '2020-04-01'
			and
			nth_order >=2
		group by
			cohort,
			country_abbreviation,
			country_fullname,
			date(created_at)::timestamp without time zone,
			init_order_incl_starter_set,
			init_order_incl_pods
		),

	one_side_groupby as
		(select
			cohort,
			country_abbreviation,
			country_fullname,
			date(created_at)::timestamp without time zone as date,
			init_order_incl_starter_set, -- dim 1
			'all', -- dim 2
			count(distinct customer_id) as customers,
			count(distinct order_number) as orders
		from
			data_prep_2_by_country
		where
			cohort >= '2020-04-01'
			and
			nth_order >=2
		group by
			cohort,
			country_abbreviation,
			country_fullname,
			date(created_at)::timestamp without time zone,
			init_order_incl_starter_set
		),

	full_agg as
		(select
			cohort,
			country_abbreviation,
			country_fullname,
			date(created_at)::timestamp without time zone as date,
			'all', -- dim 1
			'all', -- dim 2
			count(distinct customer_id) as customers,
			count(distinct order_number) as orders
		from
			data_prep_2_by_country
	    where cohort >= '2020-04-01'
            and (nth_order >= 2
            or (nth_order = 1 and pods >= 1))
		group by
			cohort,
			country_abbreviation,
			country_fullname,
			date(created_at)::timestamp without time zone,
			init_order_incl_starter_set
		),

		union_cte as (
			select * from main_groupby
			union all
			select * from one_side_groupby
			union all
			select * from full_agg)

		--- ##############################
		--- ### end of the rollup replacement
		--- ##############################

--- ##############################
--- ### final query calculating the retention
--- ##############################
select
	union_cte.country_abbreviation,
	union_cte.country_fullname,
	union_cte.cohort,
	union_cte."date",
	union_cte.init_order_incl_starter_set,
	union_cte.init_order_incl_pods,
    -- ### calculating cumulative orders on a daily granularity; tableau then calls MAX(orders) for each month
	sum(union_cte.orders)
	    over (partition by
	        union_cte.cohort,
	        union_cte.init_order_incl_starter_set,
	        union_cte.init_order_incl_pods,
	        union_cte.country_abbreviation
		order by union_cte."date" asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
	as orders_pods_first,
    -- ### calculating non-cumulative orders on a monthly granularity (same value for each month)
	sum(union_cte.orders)
		over (partition by
			union_cte.cohort,
		    date_trunc('month'::text, union_cte.date),
			union_cte.init_order_incl_starter_set,
			union_cte.init_order_incl_pods,
		    union_cte.country_abbreviation)
	as noncumulative_orders_pods_first
from
	union_cte