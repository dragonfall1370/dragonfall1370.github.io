---######################################
---### this view includes basket of customer who bought Pod case
---#####################################




select * from "airup_eu_dwh"."dbt_nhamdao"."fct_pod_case_processing"
where include_pod_case =1