

  create view "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_first_order__dbt_tmp" as (
    

select * from  "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_first_order_products"
union all
select * from  "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_first_order_drinking_systems"
  ) with no schema binding;
