

  create view "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate_global__dbt_tmp" as (
    --- #### Prep views for the current retention logic (old rebuy rate logic) defined in 2/2022
--- #### Parts of the query carry unclear legacy decisions and it is not optimized for performance
--- #### Created by: Feld M
--- #### Last edited by: Tomas K., Feld M on 1.2.2022



with
    --- ##############################
    --- ### gathers number of SS, pods & accessories in each order
	--- ### adding the new mapping table influence the results (1.2.2022 new/old:SS 1429766/1049917, pods 4067395/4182940, acc 347727/302167)
    --- ##############################
	product_categories_per_order as		-- To Do: remove this CTE and migrate relevant fields to data_prep
		(select
			fct_order_enriched.id as "oid",
			sum(case when shopify_product_categorisation.subcategory_1 = 'Starter Set' then 1 end) as starter_sets,
			sum(case when shopify_product_categorisation.category = 'Flavour' then 1 end) as pods,
			sum(case when shopify_product_categorisation.category = 'Accessories' then 1 end) as accessories
		from
			"airup_eu_dwh"."shopify_global"."fct_order_enriched" fct_order_enriched
		left join "airup_eu_dwh"."shopify_global"."fct_order_line" fct_order_line 
			on fct_order_enriched.id = fct_order_line.order_id
		left join "airup_eu_dwh"."shopify_global"."seed_shopify_product_categorisation" shopify_product_categorisation 
			on fct_order_line.sku = shopify_product_categorisation.sku
		group by
			1
		),

    --- ##############################
    --- ### CTE doing most of the heavy lifting, e.g., date diff, nth_order, leads
    --- ##############################
	data_prep as
		(select distinct
			customer_id,
			id as "oid",
			sum(1) over (partition by customer_id order by created_at asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as nth_order,
			created_at,
			case
				when (sum(1) over (partition by customer_id order by created_at asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and starter_sets >= 1
				then 1
				else 0
			end as init_order_starter_set,
			case
				when (sum(1) over (partition by customer_id order by created_at asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1 and pods >= 1
				then 1
				else 0
			end as init_order_pods,
			lead(created_at) over (partition by customer_id order by created_at asc) created_at_next_order,
			lead(created_at) over (partition by customer_id order by created_at asc) - created_at as time_diff_next_order,
			min(date(date_trunc('month', created_at))) over (partition by customer_id) as cohort,
			date_trunc('month', created_at) as month_created_at,
			-- ##############################
			-- calculate date diff in months
			(DATE_PART('year', created_at::date)
			- DATE_PART('year', (min(date_trunc('month', created_at)) over (partition by customer_id))::date))
			* 12 +
			(DATE_PART('month', created_at::date)
			- DATE_PART('month', (min(date_trunc('month', created_at)) over (partition by customer_id))::date)) as date_diff_months,
			-- ##############################
			sum(net_revenue_2) AS net_revenue,
			starter_sets,
			pods,
			accessories,
			pods >= 1 as "pod_order"
		from
			"airup_eu_dwh"."shopify_global"."fct_order_enriched" fct_order_enriched
		left join product_categories_per_order on
			fct_order_enriched.id = product_categories_per_order."oid"
		where
			fct_order_enriched.financial_status in ('paid', 'partially_refunded')
		group by
			customer_id,
			id,
			created_at,
			starter_sets,
			pods,
			accessories
			),


    --- ##############################
    --- ### CTE flagging the whole customers whether init SS / pods
    --- ### and counting orders, pods and pods orders
    --- ##############################
	data_prep_2 as
		(select
			data_prep.*,
			sum(1) over (partition by customer_id order by created_at, oid asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as order_per_client,
			case
				when max(init_order_starter_set) over (partition by customer_id) = 1
				then 'yes'
				else 'no'
			end as init_order_incl_starter_set,
			case
				when max(init_order_pods) over (partition by customer_id) = 1
				then 'yes'
				else 'no'
			end as init_order_incl_pods,
			sum(pods) over (partition by customer_id) as pod_items_per_customer,
			count(pods) over (partition by customer_id, date_diff_months) as pod_orders_per_customer
		from
			data_prep),

	--- ##############################
	--- ### ROLL UP replacement added after migration
	--- ##############################
	main_groupby as
		(select
			cohort,
			date(created_at)::timestamp without time zone as date,
			init_order_incl_starter_set::text, -- dim 1
			init_order_incl_pods::text, -- dim 2
			count(DISTINCT data_prep_2."oid")  AS orders
		from
			data_prep_2
		where
			cohort >= '2020-04-01'
			and
			nth_order >= 2
		group by
			cohort,
			date(created_at)::timestamp without time zone,
			init_order_incl_starter_set,
			init_order_incl_pods
		),

	one_side_groupby as
		(select
			cohort,
		    date,
			init_order_incl_starter_set::text,
			'all'::text as init_order_incl_pods, -- dim 2
			sum(orders) as orders
		from
			main_groupby
		group by
			1,2,3,4
		),

	full_agg as
		(select
			cohort,
		    date,
			'all'::text init_order_incl_starter_set,
			'all'::text as init_order_incl_pods, -- dim 2
			sum(orders) as orders
		from
			main_groupby
		group by
			1,2,3,4
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
	union_cte.cohort,
	union_cte."date",
	union_cte.init_order_incl_starter_set,
	union_cte.init_order_incl_pods,
    -- ### calculating cumulative orders on a daily granularity; tableau then calls MAX(orders) for each month
	sum(union_cte.orders)
	    over (partition by
	        union_cte.cohort,
	        union_cte.init_order_incl_starter_set,
	        union_cte.init_order_incl_pods
		order by union_cte."date" asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
	as orders,
    -- ### calculating non-cumulative orders on a monthly granularity (same value for each month)
	sum(union_cte.orders)
		over (partition by
			union_cte.cohort,
		    date_trunc('month'::text, union_cte.date),
			union_cte.init_order_incl_starter_set,
			union_cte.init_order_incl_pods)
	as noncumulative_orders
from
	union_cte
  ) with no schema binding;
