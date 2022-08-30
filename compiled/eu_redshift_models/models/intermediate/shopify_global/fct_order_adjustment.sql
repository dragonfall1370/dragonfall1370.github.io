 

SELECT  
       _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_de"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'Base')
  
      
UNION all

SELECT  
       _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_at"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'AT')
  
      
UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_fr"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'FR')
  
   
UNION all

 SELECT 
     _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_nl"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'NL')
  

UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_ch"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_uk"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'UK')
  
   
UNION all

 SELECT 

      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_it"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'IT')
  


UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,null as amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,null as tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_se"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'SE')
  



UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_usd
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_usd
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_us"


            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."fct_order_adjustment" where 1=1 and shopify_shop = 'US')
  