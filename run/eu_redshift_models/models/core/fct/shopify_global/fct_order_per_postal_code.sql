

  create view "airup_eu_dwh"."shopify_global"."fct_order_per_postal_code__dbt_tmp" as (
    --created by: Nham Dao
--this view provide information of orders per postal code
 

select shipping_address_zip, shipping_address_city, shipping_address_country, created_at::date as created_date, count(*) as number_of_order,
sum(net_revenue_2) as net_revenue_2
from "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe 
where financial_status in ('paid', 'partially_refunded')
group by shipping_address_zip, shipping_address_city, shipping_address_country, created_at::date
  ) with no schema binding;
