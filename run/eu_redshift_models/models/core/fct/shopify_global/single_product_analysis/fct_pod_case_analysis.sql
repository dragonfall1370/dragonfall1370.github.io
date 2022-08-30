

  create view "airup_eu_dwh"."dbt_nhamdao"."fct_pod_case_analysis__dbt_tmp" as (
    ---######################################
---### this view includes basket of customer who bought Pod case
---#####################################




with summary_data as (select customer_id, country, region, first_order_date, nth_order, order_id
,net_revenue_2, include_pod_case, number_of_distinct_product, created_at, month_diff, day_diff, first_purchase_incl_drinking_system,rank_order_per_cust
from dbt_nhamdao.fct_pod_case_processing
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14)
select * from summary_data
  ) with no schema binding;
