

  create view "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate_pods_first__dbt_tmp" as (
    --- #### View combines global and country level cohort analysis (https://app.asana.com/0/1199880849627986/1200103049337654)
--- #### Created by: Thomas B., Feld M
--- #### Last edited by: Tomas K., Feld M on 1.2.2022



SELECT
	a.country_abbreviation,
	a.country_fullname,
	a.cohort,
	a."date",
	a.init_order_incl_starter_set,
	a.init_order_incl_pods,
	a.customers_pods_first,
    a.noncumulative_customers_pods_first
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate_pods_first_by_country" a
UNION ALL
SELECT
	'all' AS country_abbreviation,
	'all' AS country_fullname,
	b.cohort,
	b."date",
	b.init_order_incl_starter_set,
	b.init_order_incl_pods,
	b.customers_pods_first,
    b.noncumulative_customers_pods_first
FROM
    "airup_eu_dwh"."shopify_global"."fct_all_cohort_retention_rate_pods_first_global" b
  ) with no schema binding;
