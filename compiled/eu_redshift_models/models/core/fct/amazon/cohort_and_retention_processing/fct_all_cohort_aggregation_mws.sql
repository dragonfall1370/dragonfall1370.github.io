

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
			"airup_eu_dwh"."amazon"."fct_mws_cohort_processing"
		where
			date_diff_months = 0
		group by
			country_abbreviation,
			country_fullname,
			cohort,
			init_order_incl_starter_set,
			init_order_incl_pods),

	one_side_groupby as (
		select
			country_abbreviation,
			country_fullname,
			cohort,
			sum(cohort_customers) as cohort_customers,
			init_order_incl_starter_set,
			'all' as init_order_incl_pods
		from
			main_groupby
			--"airup_eu_dwh"."amazon"."fct_mws_cohort_processing"
		group by
			country_abbreviation,
			country_fullname,
			cohort,
			init_order_incl_starter_set
	),

	full_agg as (
		select
			country_abbreviation,
			country_fullname,
			cohort,
			sum(cohort_customers) as cohort_customers,
			'all' as init_order_incl_starter_set,
			'all' as init_order_incl_pods
		from
			main_groupby
			--"airup_eu_dwh"."amazon"."fct_mws_cohort_processing"
		group by
			country_abbreviation,
			country_fullname,
			cohort
	),

	-- union_cte
	all_cohort_customer_groups_mws as (
 	select * from main_groupby
    union all
    select * from one_side_groupby
    union all
    select * from full_agg)

select
    --- ### initial spine
	all_cohort_retention_rate_mws_new.country_abbreviation,
	all_cohort_retention_rate_mws_new.country_fullname,
	all_cohort_retention_rate_mws_new.cohort,
	all_cohort_retention_rate_mws_new."date",
	all_cohort_retention_rate_mws_new.init_order_incl_starter_set,
	all_cohort_retention_rate_mws_new.init_order_incl_pods,
    --- ### cumulative orders
	all_cohort_retention_rate_mws_new.orders,
	all_cohort_retention_rate_mws_new.orders / cohort_customers::float as retention_rate_cumulative,
	all_cohort_retention_rate_pods_first_mws_new.orders_pods_first,
	all_cohort_retention_rate_pods_first_mws_new.orders_pods_first / cohort_customers::float as retention_rate_pods_first_cumulative,
    --- ### non cumulative orders
	all_cohort_retention_rate_mws_new.noncumulative_orders,
	all_cohort_retention_rate_mws_new.noncumulative_orders / cohort_customers::float as retention_rate_noncumulative,
	all_cohort_retention_rate_pods_first_mws_new.noncumulative_orders_pods_first,
	all_cohort_retention_rate_pods_first_mws_new.noncumulative_orders_pods_first / cohort_customers::float as retention_rate_pods_first_noncumulative,
    --- ### additional metrics
	all_cohort_customer_groups_mws.cohort_customers as cohort_size
from
    "airup_eu_dwh"."amazon"."fct_all_cohort_retention_rate_mws" all_cohort_retention_rate_mws_new
left join all_cohort_customer_groups_mws on
	all_cohort_retention_rate_mws_new.cohort = all_cohort_customer_groups_mws.cohort
	and
	all_cohort_retention_rate_mws_new.init_order_incl_starter_set = all_cohort_customer_groups_mws.init_order_incl_starter_set
	and
	all_cohort_retention_rate_mws_new.init_order_incl_pods = all_cohort_customer_groups_mws.init_order_incl_pods
	and
	all_cohort_retention_rate_mws_new.country_abbreviation = all_cohort_customer_groups_mws.country_abbreviation
left join "airup_eu_dwh"."amazon"."fct_all_cohort_retention_rate_pods_first_mws" all_cohort_retention_rate_pods_first_mws_new on
	all_cohort_retention_rate_mws_new.date = all_cohort_retention_rate_pods_first_mws_new.date
	AND
	all_cohort_retention_rate_mws_new.cohort = all_cohort_retention_rate_pods_first_mws_new.cohort
	AND
	all_cohort_retention_rate_mws_new.init_order_incl_starter_set = all_cohort_retention_rate_pods_first_mws_new.init_order_incl_starter_set
	AND
	all_cohort_retention_rate_mws_new.init_order_incl_pods = all_cohort_retention_rate_pods_first_mws_new.init_order_incl_pods
	AND
	all_cohort_retention_rate_mws_new.country_abbreviation::text = all_cohort_retention_rate_pods_first_mws_new.country_abbreviation::text