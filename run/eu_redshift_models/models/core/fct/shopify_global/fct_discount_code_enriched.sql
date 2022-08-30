

  create view "airup_eu_dwh"."shopify_global"."fct_discount_code_enriched__dbt_tmp" as (
    ---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################
        -- this view contains the information on the discount code, valid days, type and value
---###################################################################################################################

 

 with discount_code_fr as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'fr' as country_code from "airup_eu_dwh"."shopify_fr"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_fr"."price_rule" pr on dc.price_rule_id=pr.id), 
 discount_code_ch as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'ch' as country_code from "airup_eu_dwh"."shopify_ch"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_ch"."price_rule" pr on dc.price_rule_id=pr.id ), 
 discount_code_at as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'at' as country_code from "airup_eu_dwh"."shopify_at"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_at"."price_rule" pr on dc.price_rule_id=pr.id ), 
 discount_code_de as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'de' as country_code from "airup_eu_dwh"."shopify_de_nrt"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_de_nrt"."price_rule" pr on dc.price_rule_id=pr.id ), 
 discount_code_it as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'it' as country_code from "airup_eu_dwh"."shopify_it"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_it"."price_rule" pr on dc.price_rule_id=pr.id ), 
 discount_code_nl as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'nl' as country_code from "airup_eu_dwh"."shopify_nl"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_nl"."price_rule" pr on dc.price_rule_id=pr.id ), 
 discount_code_uk as 
 (select dc.code, pr.starts_at, pr.ends_at, pr.title, pr.value_type, pr.value, 'uk' as country_code from "airup_eu_dwh"."shopify_uk"."discount_code" dc 
 left join "airup_eu_dwh"."shopify_uk"."price_rule" pr on dc.price_rule_id=pr.id )
 select * from discount_code_fr
 union 
 select * from discount_code_ch
  union 
 select * from discount_code_at
  union 
 select * from discount_code_it
  union 
 select * from discount_code_nl
  union 
 select * from discount_code_uk
 union 
 select * from discount_code_de
  ) with no schema binding;
