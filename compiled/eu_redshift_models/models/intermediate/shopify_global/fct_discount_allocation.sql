 

with final as (

SELECT 
      _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_de"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'Base')
  
   
UNION all

SELECT 
      _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_at"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'AT')
  
   
UNION all

 SELECT 
      _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_fr"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'FR')
  
   
UNION all

 SELECT 
     _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_nl" 
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'NL')
  

UNION all

 SELECT 
      _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_ch"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
     _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_uk"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'UK')
  
   
UNION all

 SELECT 
    _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_it"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'IT')
  

UNION all

 SELECT 
     _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 null as amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 null as amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 null as amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_se"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'SE')
  

UNION all

 SELECT 
     _fivetran_synced,
         creation_date,
	     amount,
	     amount_chf,
	     amount_gbp,
		 amount_sek,
		 amount_usd,
	     amount_set_presentment_money_amount,
	     amount_set_presentment_money_amount_chf,
	     amount_set_presentment_money_amount_gbp,
		 amount_set_presentment_money_amount_sek,
		 amount_set_presentment_money_amount_usd,
	     amount_set_presentment_money_currency_code,
	     amount_set_shop_money_amount,
	     amount_set_shop_money_amount_chf,
	     amount_set_shop_money_amount_gbp,
		 amount_set_shop_money_amount_sek,
		 amount_set_shop_money_amount_usd,
	     amount_set_shop_money_currency_code,
	     discount_application_index,
	     index,
	     order_line_id,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."discount_allocation_us"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."fct_discount_allocation" where 1=1 and shopify_shop = 'US')
  


)

select *
from final