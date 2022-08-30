----legacy: amazon.amazon.mws_cohort_processing_new
---Authors: Etoma Egot

---###################################################################################################################

        ---compute all_cohort_retention_rate_pods_first_mws_by_country---
        -- CW 16 2021 new view to combine global and country level cohort analysis (https://app.asana.com/0/1199880849627986/1200103049337654)

---###################################################################################################################

 


SELECT
    country_abbreviation,
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
     "airup_eu_dwh"."amazon"."fct_mws_cohort_processing_by_country"

UNION ALL

SELECT
    'all' AS country_abbreviation,
    'all' AS country_fullname,
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
   "airup_eu_dwh"."amazon"."fct_mws_cohort_processing_global"