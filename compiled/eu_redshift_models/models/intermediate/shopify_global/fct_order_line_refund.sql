 


SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_de"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'Base')
  
   
UNION all

SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_at"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'AT')
  
   
UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_fr"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'FR')
  
   
UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_nl"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'NL')
  

UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_ch"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_uk"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'UK')
  

   UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_se"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'SE')
  

   UNION all
   

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     null as subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     null as total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_it"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'IT')
  

   UNION all
   
 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
	 subtotal_sek,
     subtotal_usd,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
	 total_tax_sek,
     total_tax_usd,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
	conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_us"


            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."fct_order_line_refund" where 1=1 and shopify_shop = 'US')
  