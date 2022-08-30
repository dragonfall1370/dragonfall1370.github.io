

--- ##############################
--- ### ROLL UP replacement added after migration
--- ##############################

with
	main_groupby as
		(select
			country_abbreviation,
			country_fullname,
			cohort,
			sum(nr_of_customers) as cohort_customers,
			init_order_incl_starter_set,
			init_order_incl_pods
		from
			shopify_global.fct_shopify_cohort_processing
		where
			date_diff_months = 0
		group by
			country_abbreviation,
			country_fullname,
			cohort,
			init_order_incl_starter_set,
			init_order_incl_pods),

	one_side_groupby1 as (
		select
			country_abbreviation,
			country_fullname,
			cohort,
			sum(cohort_customers) as cohort_customers,
			init_order_incl_starter_set,
			CAST('all' as text) as init_order_incl_pods
		from
			main_groupby
		group by
			country_abbreviation,
			country_fullname,
			cohort,
			init_order_incl_starter_set
	),
	one_side_groupby2 as (
		select
			country_abbreviation,
			country_fullname,
			cohort,
			sum(cohort_customers) as cohort_customers,			
			CAST('all' as text) as init_order_incl_starter_set,
			init_order_incl_pods
		from
			main_groupby
		group by
			country_abbreviation,
			country_fullname,
			cohort,
			init_order_incl_pods
	),

	full_agg as (
		select
			country_abbreviation,
			country_fullname,
			cohort,
			sum(cohort_customers) as cohort_customers,
			CAST('all' as text) as init_order_incl_starter_set,
			CAST('all' as text) as init_order_incl_pods
		from
			main_groupby
		group by
			1,2,3,5,6
	),	

	-- union_cte
	all_cohort_customer_groups as (
 	select
		cast(country_abbreviation as text),
		cast(country_fullname as text),
		cast(cohort as text),
		cast(cohort_customers as text),
		cast(init_order_incl_starter_set as text),
		cast(init_order_incl_pods as text)
	from main_groupby
 	union
  	select
		cast(country_abbreviation as text),
		cast(country_fullname as text),
		cast(cohort as text),
		cast(cohort_customers as text),
		cast(init_order_incl_starter_set as text),
		cast(init_order_incl_pods as text)
	from one_side_groupby1
	union
  	select
		cast(country_abbreviation as text),
		cast(country_fullname as text),
		cast(cohort as text),
		cast(cohort_customers as text),
		cast(init_order_incl_starter_set as text),
		cast(init_order_incl_pods as text)
	from one_side_groupby2
  	union
  	select
		cast(country_abbreviation as text),
		cast(country_fullname as text),
		cast(cohort as text),
		cast(cohort_customers as text),
		cast(init_order_incl_starter_set as text),
		cast(init_order_incl_pods as text)
	from full_agg

  ), 
  all_data as (SELECT all_cohort_retention_rate.country_abbreviation,
		   all_cohort_retention_rate.country_fullname,
		   all_cohort_retention_rate.cohort,
		   all_cohort_retention_rate.date,
		   all_cohort_retention_rate.init_order_incl_starter_set,
		   all_cohort_retention_rate.init_order_incl_pods,
		   all_cohort_retention_rate.customers,
		   all_cohort_retention_rate.customers::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS retention_rate,
		   all_cohort_retention_rate.noncumulative_customers,
		   all_cohort_retention_rate.noncumulative_customers::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS retention_rate_noncumulative,
		   all_cohort_retention_rate_pods_first.customers_pods_first,
		   all_cohort_retention_rate_pods_first.customers_pods_first::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS retention_rate_pods_first,
		   all_cohort_retention_rate_pods_first.noncumulative_customers_pods_first,
		   all_cohort_retention_rate_pods_first.noncumulative_customers_pods_first::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS retention_rate_pod_first_noncumulative,
		   all_cohort_rebuy_rate.orders,
		   all_cohort_rebuy_rate.orders::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS rebuy_rate,
		   all_cohort_rebuy_rate.noncumulative_orders,
		   all_cohort_rebuy_rate.noncumulative_orders::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS rebuy_rate_noncumulative,
		   all_cohort_rebuy_rate_pods_first.orders_pods_first,
		   all_cohort_rebuy_rate_pods_first.orders_pods_first::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS rebuy_rate_pods_first,
		   all_cohort_rebuy_rate_pods_first.noncumulative_orders_pods_first,
		   all_cohort_rebuy_rate_pods_first.noncumulative_orders_pods_first::double precision /
		   all_cohort_customer_groups.cohort_customers::double precision AS rebuy_rate_pods_first_noncumulative,
		   all_cohort_customer_groups.cohort_customers                   AS cohort_size
	FROM "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate" all_cohort_retention_rate
			 LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate_pods_first" all_cohort_retention_rate_pods_first
					   ON all_cohort_retention_rate.date = all_cohort_retention_rate_pods_first.date AND
						  all_cohort_retention_rate.cohort = all_cohort_retention_rate_pods_first.cohort AND
						  all_cohort_retention_rate.init_order_incl_starter_set =
						  all_cohort_retention_rate_pods_first.init_order_incl_starter_set AND
						  all_cohort_retention_rate.init_order_incl_pods =
						  all_cohort_retention_rate_pods_first.init_order_incl_pods AND
						  all_cohort_retention_rate.country_abbreviation::text =
						  all_cohort_retention_rate_pods_first.country_abbreviation::text
			 LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate" all_cohort_rebuy_rate
					   ON all_cohort_retention_rate.date = all_cohort_rebuy_rate.date AND
						  all_cohort_retention_rate.cohort = all_cohort_rebuy_rate.cohort AND
						  all_cohort_retention_rate.init_order_incl_starter_set =
						  all_cohort_rebuy_rate.init_order_incl_starter_set AND
						  all_cohort_retention_rate.init_order_incl_pods =
						  all_cohort_rebuy_rate.init_order_incl_pods AND
						  all_cohort_retention_rate.country_abbreviation::text =
						  all_cohort_rebuy_rate.country_abbreviation::text
			 LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate_pods_first" all_cohort_rebuy_rate_pods_first
					   ON all_cohort_retention_rate.date = all_cohort_rebuy_rate_pods_first.date AND
						  all_cohort_retention_rate.cohort = all_cohort_rebuy_rate_pods_first.cohort AND
						  all_cohort_retention_rate.init_order_incl_starter_set =
						  all_cohort_rebuy_rate_pods_first.init_order_incl_starter_set AND
						  all_cohort_retention_rate.init_order_incl_pods =
						  all_cohort_rebuy_rate_pods_first.init_order_incl_pods AND
						  all_cohort_retention_rate.country_abbreviation::text =
						  all_cohort_rebuy_rate_pods_first.country_abbreviation::text
			 LEFT JOIN all_cohort_customer_groups
					   ON all_cohort_retention_rate.cohort = all_cohort_customer_groups.cohort AND
						  all_cohort_retention_rate.init_order_incl_starter_set =
						  all_cohort_customer_groups.init_order_incl_starter_set AND
						  all_cohort_retention_rate.init_order_incl_pods =
						  all_cohort_customer_groups.init_order_incl_pods AND
						  all_cohort_retention_rate.country_abbreviation::text =
						  all_cohort_customer_groups.country_abbreviation::text)
	select all_data.*, country_mapping.country_grouping from all_data
left join "airup_eu_dwh"."public"."country_system_account_mapping" as country_mapping
on country_mapping.country_abbreviation = all_data.country_abbreviation