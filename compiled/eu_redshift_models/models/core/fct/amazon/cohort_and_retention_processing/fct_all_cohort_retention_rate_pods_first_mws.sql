

SELECT all_cohort_retention_rate_pods_first_mws_by_country.country_abbreviation,
    all_cohort_retention_rate_pods_first_mws_by_country.country_fullname,
    all_cohort_retention_rate_pods_first_mws_by_country.cohort,
    all_cohort_retention_rate_pods_first_mws_by_country.date,
    all_cohort_retention_rate_pods_first_mws_by_country.init_order_incl_starter_set,
    all_cohort_retention_rate_pods_first_mws_by_country.init_order_incl_pods,
    all_cohort_retention_rate_pods_first_mws_by_country.orders_pods_first,
    all_cohort_retention_rate_pods_first_mws_by_country.noncumulative_orders_pods_first
   FROM "airup_eu_dwh"."amazon"."fct_all_cohort_retention_rate_pods_first_mws_by_country" all_cohort_retention_rate_pods_first_mws_by_country
UNION ALL 
 SELECT 'all'::character varying AS country_abbreviation,
    'all'::character varying AS country_fullname,
    all_cohort_retention_rate_pods_first_mws_global.cohort,
    all_cohort_retention_rate_pods_first_mws_global.date,
    all_cohort_retention_rate_pods_first_mws_global.init_order_incl_starter_set,
    all_cohort_retention_rate_pods_first_mws_global.init_order_incl_pods,
    all_cohort_retention_rate_pods_first_mws_global.orders_pods_first,
    all_cohort_retention_rate_pods_first_mws_global.noncumulative_orders_pods_first
   FROM "airup_eu_dwh"."amazon"."fct_all_cohort_retention_rate_pods_first_mws_global" all_cohort_retention_rate_pods_first_mws_global