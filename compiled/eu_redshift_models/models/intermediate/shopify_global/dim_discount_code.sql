 

 SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'Base'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_de_nrt"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'Base')
  
   
   
UNION all

SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'AT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_at"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'AT')
  

   UNION all

---#########################################################################################################################

            ------------------------------FRANCE
            

---#########################################################################################################################

 SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'FR'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_fr"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'FR')
  



   UNION all

   
---#########################################################################################################################

            ------------------------------NETHERLANDS
            

---#########################################################################################################################

   SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'NL'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_nl"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'NL')
  

---#########################################################################################################################

            ------------------------------SWITZERLAND
            

---#########################################################################################################################

UNION all
  

SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'CH'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_ch"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'CH')
  
  
   
  UNION All
 
   
---#########################################################################################################################

            ------------------------------UNITED KINGDOM
            

---#########################################################################################################################

    SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'UK'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_uk"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'UK')
  
   
UNION All
 
   
---#########################################################################################################################

            ------------------------------ITALY
            

---#########################################################################################################################

    SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'IT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_it"."discount_code"
                -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'IT')
  

   UNION ALL

   ---#########################################################################################################################

            ------------------------------SWEDEN
            

---#########################################################################################################################

    SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'SE'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_se"."discount_code"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'SE')
  


   UNION ALL

   ---#########################################################################################################################

            ------------------------------USA
            

---#########################################################################################################################

    SELECT 

    discount_code._fivetran_synced,
    discount_code.id,
    discount_code.price_rule_id,
    discount_code.code,
    discount_code.created_at,
    discount_code.updated_at,
    discount_code.usage_count,
    'US'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_us"."discount_code"

            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_code" where 1=1 and shopify_shop = 'US')
  