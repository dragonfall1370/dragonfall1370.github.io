

  create view "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate__dbt_tmp" as (
    --- #### View combines global and country level cohort analysis (https://app.asana.com/0/1199880849627986/1200103049337654)
--- #### Created by: Thomas B., Feld M
--- #### Last edited by: Tomas K., Feld M on 1.2.2022



SELECT
	country_abbreviation,
	country_fullname,
	cohort,
	"date",
	init_order_incl_starter_set,
	init_order_incl_pods,
	customers,
	noncumulative_customers
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate_by_country"
UNION ALL
SELECT
	'all' AS country_abbreviation,
	'all' AS country_fullname,
	cohort,
	"date",
	init_order_incl_starter_set,
	init_order_incl_pods,
	customers,
	noncumulative_customers
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate_global"
  ) with no schema binding;
