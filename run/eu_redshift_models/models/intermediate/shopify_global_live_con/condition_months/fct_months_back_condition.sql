

  create  table
    "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition__dbt_tmp"
    
    
    
  as (
     

select three_month::date as three_month_ago
from (select date_trunc('month', GETDATE()) - interval '3 month' as three_month)
  );