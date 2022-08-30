

SELECT
	country_abbreviation,
	country_fullname,
	cohort,
	"date",
	init_order_incl_starter_set,
	init_order_incl_pods,
	orders_pods_first,
	noncumulative_orders_pods_first
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate_pods_first_by_country"
UNION ALL
SELECT
	'all' AS country_abbreviation,
	'all' AS country_fullname,
	cohort,
	"date",
	init_order_incl_starter_set,
	init_order_incl_pods,
	orders_pods_first,
	noncumulative_orders_pods_first
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate_pods_first_global"