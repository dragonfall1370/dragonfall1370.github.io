

  create view "airup_eu_dwh"."zzz_long_test"."fct_shopify_product_indirect_retention_all_orders__dbt_tmp" as (
    select 1 from "airup_eu_dwh"."dbt_feldm"."fct_shopify_cohort_processing_new"
  ) with no schema binding;
