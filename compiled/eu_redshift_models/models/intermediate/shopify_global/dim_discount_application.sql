
   

with final as (

SELECT 
        hash_id,
        _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_de"
    -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'Base')
  
          
UNION all

SELECT 
        hash_id,
        _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_at"
        -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'AT')
  
          
UNION all

 SELECT 
        hash_id,
       _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_fr"
           -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'FR')
  

UNION all

 SELECT 
         hash_id,
        _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_nl"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'NL')
  

UNION all

 SELECT
       hash_id,
      _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_ch"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
       hash_id,
     _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_uk"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'UK')
  
   
UNION all

 SELECT 
       hash_id,
       _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,        
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_it"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'IT')
  
UNION all

 SELECT 
       hash_id,
       _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        null as value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_se"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'SE')
  

UNION all

 SELECT 
       hash_id,
       _fivetran_synced,
        creation_date,
        target_type,
        target_selection,
        allocation_method,
        value_type,
        description,
        code,
        title,
        type,
        index,
        order_id,
        value,
		value_sek,
        value_chf,
	    value_gbp,
        value_usd,
	    shopify_shop,
		currency_abbreviation,
		conversion_rate_eur
    FROM "airup_eu_dwh"."shopify_global"."discount_application_us"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."dim_discount_application" where 1=1 and shopify_shop = 'US')
  

)

select *
from final

-----incremental table macro---
--   
--   where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."dim_discount_application")
--   