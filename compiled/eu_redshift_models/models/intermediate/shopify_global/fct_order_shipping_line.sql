 

with final as (

SELECT 
      _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_de"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'Base')
  
   
UNION all

SELECT 
      _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_at"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'AT')
  
   
UNION all

 SELECT 
      _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_fr"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'FR')
  

UNION all

 SELECT 
    _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_nl"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'NL')
  

UNION all

 SELECT 
     _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_ch"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
     _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_uk"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'UK')
  
   
UNION all

 SELECT 
      _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_it"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'IT')
  

UNION all

 SELECT 
      _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  null as discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  null as price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_se"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'SE')
  


UNION all

 SELECT 
      _fivetran_synced,
	     creation_date,
	     carrier_identifier,
	     code,
	     delivery_category,
	     discounted_price,
	     discounted_price_chf,
	     discounted_price_gbp,
		  discounted_price_sek,
		  discounted_price_usd,
	     discounted_price_set,
	     id,
	     order_id,
	     phone,
	     price,
	     price_chf,
	     price_gbp,
		  price_sek,
		  price_usd,
	     price_set,
	     requested_fulfillment_service_id,
	     source,
	     title,
	     shopify_shop,
	     currency_abbreviation,
	     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_us"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" where 1=1 and shopify_shop = 'US')
  


)

select *
from final