


SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'Base'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_de"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'Base')
  

UNION ALL

SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'AT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_at"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'AT')
  

UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'FR'::text AS shopify_shop
    
   FROM "airup_eu_dwh"."shopify_fr"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'FR')
  

UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'NL'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_nl"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'NL')
  

UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'CH'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_ch"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'CH')
  

UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'UK'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_uk"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'UK')
  

UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'IT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_it"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'IT')
  

UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'US'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_us"."inventory_item"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'US')
  


      UNION ALL
 SELECT inventory_item._fivetran_synced,
    inventory_item.cost,
    inventory_item.country_code_of_origin,
    inventory_item.created_at,
    inventory_item.id,
    inventory_item.province_code_of_origin,
    inventory_item.requires_shipping,
    inventory_item.sku,
    inventory_item.tracked,
    inventory_item.updated_at,
    'SE'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_se"."inventory_item"

            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_inventory_item" where 1=1 and shopify_shop = 'SE')
  