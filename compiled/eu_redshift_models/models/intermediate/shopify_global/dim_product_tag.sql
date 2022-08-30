 

with final as 
(
SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'Base'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_de"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'Base')
  
   
   
UNION ALL

SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'AT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_at"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'AT')
  
   
   
UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'FR'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_fr"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'FR')
  
   
UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'NL'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_nl"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'NL')
  
   
UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'CH'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_ch"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'CH')
  
    
UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'UK'::text AS shopify_shop
  FROM "airup_eu_dwh"."shopify_uk"."product_tag"
              -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'UK')
  
   
UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'IT'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_it"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'IT')
  


UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'US'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_us"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'US')
  

UNION ALL
 SELECT product_tag._fivetran_synced,
    product_tag.index,
    product_tag.product_id,
    product_tag.value,
    'US'::text AS shopify_shop
   FROM "airup_eu_dwh"."shopify_se"."product_tag"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_tag" where 1=1 and shopify_shop = 'SE')
  

)


select 
md5(cast(coalesce(cast(index as varchar), '') || '-' || coalesce(cast(product_id as varchar), '') as varchar)) as hash_id,
final.* 
from final