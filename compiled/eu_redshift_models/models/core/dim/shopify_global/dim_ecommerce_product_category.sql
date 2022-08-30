---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the product category and subcategory for e-commerce dashboard
---###################################################################################################################

 


select 
  case when category = 'Hardware' then 'Starter-Set'
  when category = 'Flavour' then 'Pods'
  when category = 'Accessories' then 'Accessories' 
  end as category,
  subcategory_3 as subcategory,
  sku
from "airup_eu_dwh"."shopify_global"."shopify_product_categorisation"