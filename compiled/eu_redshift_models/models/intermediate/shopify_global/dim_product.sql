 

 SELECT 
   product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'Base'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_de"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'Base')
  
   
   
UNION all

   
---#########################################################################################################################

            ------------------------------AUSTRIA
            

---#########################################################################################################################


 SELECT 
   product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'AT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_at"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'AT')
  
   
   
UNION all


   
---#########################################################################################################################

            ------------------------------FRANCE
            

---#########################################################################################################################

    SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'FR'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_fr"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'FR')
  



   UNION all

   
---#########################################################################################################################

            ------------------------------NETHERLANDS
            

---#########################################################################################################################

    SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'NL'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_nl"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'NL')
  


---#########################################################################################################################

            ------------------------------SWITZERLAND
            

---#########################################################################################################################

UNION all
  

 SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'CH'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_ch"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'CH')
  
  
   
  UNION All
 
   
---#########################################################################################################################

            ------------------------------UNITED KINGDOM
            

---#########################################################################################################################

    SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'UK'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_uk"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'UK')
  
   
UNION All
 
   
---#########################################################################################################################

            ------------------------------ITALY
            

---#########################################################################################################################

    SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'IT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_it"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'IT')
  

   UNION ALL

   ---#########################################################################################################################

            ------------------------------SWEDEN
            

---#########################################################################################################################

    SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'SE'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_se"."product"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'SE')
  

   UNION ALL

   ---#########################################################################################################################

            ------------------------------US
            

---#########################################################################################################################

    SELECT 
    product._fivetran_deleted,
    product._fivetran_synced,
    product.created_at,
    product.handle,
    product.id,
    product.product_type,
    product.published_at,
    product.published_scope,
    product.status,
    product.title,
    product.updated_at,
    product.vendor,
    'US'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_us"."product"


            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product" where 1=1 and shopify_shop = 'US')
  