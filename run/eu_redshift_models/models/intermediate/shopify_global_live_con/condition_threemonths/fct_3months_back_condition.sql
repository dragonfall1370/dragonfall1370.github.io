

  create  table
    "airup_eu_dwh"."shopify_global_live_con"."fct_3months_back_condition__dbt_tmp"
    
    
    
  as (
     

select three_month::date as three_month_ago
from (select date_trunc('month', dateadd(month,-1,GETDATE())) - interval '1 month' as three_month)
  );