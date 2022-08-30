

  create view "airup_eu_dwh"."dbt_feldm"."fct_shopify_cohort_retention_metrics_aggregation__dbt_tmp" as (
    

--#######################################
--## currently redundant view prepared to add union all for additional views / logic upstream
--#######################################

select
	*
from
	"airup_eu_dwh"."dbt_feldm"."fct_shopify_cohort_retention_metrics"
  ) with no schema binding;
