 

with final as (

SELECT 
     _fivetran_synced,
	   creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_de"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'Base')
  
   
UNION all

SELECT 
     _fivetran_synced,
	   creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_at"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'AT')
  
   
UNION all

 SELECT 
      _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_fr"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'FR')
  
   
UNION all

 SELECT 
      _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_nl"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'NL')
  

UNION all

 SELECT 
      _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_ch"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
      _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_uk"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'UK')
  
   
UNION all

 SELECT 
      _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_it"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'IT')
  

   UNION all

 SELECT 
     _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		null as price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		null as rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_se"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'SE')
  


   UNION all

 SELECT 
     _fivetran_synced,
	    creation_date,
	   index,
	   order_line_id,
	   price,    							   
	   price_chf,
	   price_gbp,
		price_sek,
		price_usd,
	   price_set,
	   rate, 
	   rate_chf,
	   rate_gbp,
		rate_sek,
		rate_usd,
	   title,
	   shopify_shop,
	   currency_abbreviation,
	   conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."tax_line_us"


            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."fct_tax_line" where 1=1 and shopify_shop = 'US')
  


)

select *
from final