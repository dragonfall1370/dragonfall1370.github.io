

  create view "airup_eu_dwh"."shopify_global"."fct_shopify_cohort_processing__dbt_tmp" as (
    --legacy:  shopify_global.shopify_cohort_processing_new

 SELECT country_abbreviation,
    country_fullname,
    cohort,
    date_diff_months,
    init_order_incl_starter_set,
    init_order_incl_pods,
    net_revenue,
    avg_pod_items_per_customer,
    avg_pod_orders_per_customer,
    nr_of_customers,
    avg_order_date_diff_days,
    subsequent_orders,
    pod_orders_per_customer,
    returning_customers_cohort,
    returning_customers_pods_first_cohort,
    subsequent_orders_with_pods,
    subsequent_orders_with_loops,
    subsequent_orders_pods_first,
    subsequent_orders_with_pods_or_loops,
    subsequent_orders_with_pods_pods_first,
    subsequent_orders_with_loops_pods_first,
    subsequent_orders_with_pods_or_loops_pods_first
   FROM 
      "airup_eu_dwh"."shopify_global"."fct_shopify_cohort_processing_by_country"


UNION ALL

 SELECT 'all'::character varying AS country_abbreviation,
    'all'::character varying AS country_fullname,
    cohort,
    date_diff_months,
    init_order_incl_starter_set,
    init_order_incl_pods,
    net_revenue,
    avg_pod_items_per_customer,
    avg_pod_orders_per_customer,
    nr_of_customers,
    avg_order_date_diff_days,
    subsequent_orders,
    pod_orders_per_customer,
    returning_customers_cohort,
    returning_customers_pods_first_cohort,
    subsequent_orders_with_pods,
    subsequent_orders_with_loops,
    subsequent_orders_pods_first,
    subsequent_orders_with_pods_or_loops,
    subsequent_orders_with_pods_pods_first,
    subsequent_orders_with_loops_pods_first,
    subsequent_orders_with_pods_or_loops_pods_first
   FROM 
        "airup_eu_dwh"."shopify_global"."fct_shopify_cohort_processing_global"
  ) with no schema binding;
