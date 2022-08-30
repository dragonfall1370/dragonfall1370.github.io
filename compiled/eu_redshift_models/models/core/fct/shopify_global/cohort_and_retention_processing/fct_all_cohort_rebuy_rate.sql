

SELECT
	country_abbreviation,
	country_fullname,
	cohort,
	"date",
	init_order_incl_starter_set,
	init_order_incl_pods,
	orders,
	noncumulative_orders
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate_by_country"
UNION ALL
SELECT
	'all' AS country_abbreviation,
	'all' AS country_fullname,
	cohort,
	"date",
	init_order_incl_starter_set,
	init_order_incl_pods,
	orders,
	noncumulative_orders
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_rebuy_rate_global"