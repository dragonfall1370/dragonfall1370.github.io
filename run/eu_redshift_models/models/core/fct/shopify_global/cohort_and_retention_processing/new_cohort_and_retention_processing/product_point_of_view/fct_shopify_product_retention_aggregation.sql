

  create  table
    "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_aggregation__dbt_tmp"
    
    
    
  as (
    

select 
    --joining dims
    all_orders.date,
    all_orders.country,
    all_orders.region,
    all_orders.product,
    all_orders.category,
    all_orders.first_ss_bundle_init_order,
    all_orders.month_diff,
    --all orders metrics
    all_orders.cohort_size as cohort_size_all_orders,
    all_orders.direct_retention_count as direct_retention_count_all_orders,
    all_orders.indirect_retention_count as indirect_retention_count_all_orders,
    --first order metrics
    first_order.cohort_size as cohort_size_first_order,
    first_order.direct_retention_count as direct_retention_count_first_order,
    first_order.indirect_retention_count as indirect_retention_count_first_order
from
    "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_all_orders" all_orders
    left join "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_first_order" first_order
        on all_orders.date = first_order.date
        and all_orders.country = first_order.country
        and all_orders.region = first_order.region
        and all_orders.product = first_order.product
        and all_orders.category = first_order.category
        and all_orders.first_ss_bundle_init_order = first_order.first_ss_bundle_init_order
        and all_orders.month_diff = first_order.month_diff
where
    -- removing redundant rows from the spine with no data
	cohort_size_all_orders is not null
	and cohort_size_first_order is not null
  );