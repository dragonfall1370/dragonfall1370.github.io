

  create view "airup_eu_dwh"."shopify_global"."fct_discount_application_enriched__dbt_tmp" as (
    ---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the order_id discount code, type and value
---###################################################################################################################

 

SELECT  da_ch.order_id, da_ch."type",  da_ch.code, da_ch.value_type, da_ch.value
   FROM "airup_eu_dwh"."shopify_ch"."discount_application" da_ch
UNION 
 SELECT da_de.order_id, da_de."type", da_de.code, da_de.value_type, da_de.value
   FROM "airup_eu_dwh"."shopify_de"."discount_application" da_de
UNION 
 SELECT da_fr.order_id, da_fr."type", da_fr.code, da_fr.value_type, da_fr.value
   FROM "airup_eu_dwh"."shopify_fr"."discount_application" da_fr
UNION 
 SELECT da_it.order_id, da_it."type", da_it.code, da_it.value_type, da_it.value
   FROM "airup_eu_dwh"."shopify_it"."discount_application" da_it
UNION 
 SELECT da_nl.order_id, da_nl."type", da_nl.code, da_nl.value_type, da_nl.value
   FROM "airup_eu_dwh"."shopify_nl"."discount_application" da_nl
UNION 
 SELECT da_se.order_id, da_se."type", da_se.code, da_se.value_type, da_se.value
   FROM "airup_eu_dwh"."shopify_se"."discount_application" da_se
UNION 
 SELECT da_uk.order_id, da_uk."type", da_uk.code, da_uk.value_type, da_uk.value
   FROM "airup_eu_dwh"."shopify_uk"."discount_application" da_uk
union 
 SELECT da_at.order_id, da_at."type", da_at.code, da_at.value_type, da_at.value
   FROM "airup_eu_dwh"."shopify_at"."discount_application" da_at
  ) with no schema binding;
