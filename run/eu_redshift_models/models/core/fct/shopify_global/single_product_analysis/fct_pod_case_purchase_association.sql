

  create view "airup_eu_dwh"."dbt_nhamdao"."fct_pod_case_purchase_association__dbt_tmp" as (
    ---######################################
---### this view includes basket of customer who bought Pod case
---#####################################




select * from "airup_eu_dwh"."dbt_nhamdao"."fct_pod_case_processing"
where include_pod_case =1
  ) with no schema binding;
