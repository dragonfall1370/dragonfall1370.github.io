
      

  create  table
    "airup_eu_dwh"."shopify_global"."dim_discount_code__dbt_tmp"
    
    
    
  as (
     

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
  
  );
  