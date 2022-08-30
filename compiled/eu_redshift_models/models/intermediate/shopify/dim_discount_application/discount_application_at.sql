
 
 with final as(
   with 
   
    discount_application_raw as (
    
   select

        discount_application._fivetran_synced,
        date(discount_application._fivetran_synced) as creation_date,
        discount_application.target_type,
        discount_application.target_selection,
        discount_application.allocation_method,
        discount_application.value_type,
        discount_application.description,
        discount_application.code,
        discount_application.title,
        discount_application.type,
        discount_application.index,
        discount_application.order_id,
        discount_application.value,
		NULL::double precision AS value_chf,
	    NULL::double precision AS value_gbp,
		NULL::double precision AS value_sek,
	    'AT'::text AS shopify_shop,
   	    'EUR'::text as currency_abbreviation   
   FROM "airup_eu_dwh"."shopify_at"."discount_application"
    

   ),
    
    global_curr_eur as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
       
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"   
       where currency_abbreviation = 'EUR'
    
    ),
  
  discount_application_enriched as (
  
   select

        discount_application_raw._fivetran_synced,
        discount_application_raw.creation_date,
        discount_application_raw.target_type,
        discount_application_raw.target_selection,
        discount_application_raw.allocation_method,
        discount_application_raw.value_type,
        discount_application_raw.description,
        discount_application_raw.code,
        discount_application_raw.title,
        discount_application_raw.type,
        discount_application_raw.index,
        discount_application_raw.order_id,
        discount_application_raw.value,
		discount_application_raw.value_chf,
	    discount_application_raw.value_gbp,
		discount_application_raw.value_sek,
	    discount_application_raw.shopify_shop,
		global_curr_eur.currency_abbreviation,
		global_curr_eur.conversion_rate_eur

    from discount_application_raw
    left join global_curr_eur using (currency_abbreviation, creation_date)  

	  )
  select * from discount_application_enriched
   )
select 
md5(cast(coalesce(cast(index as varchar), '') || '-' || coalesce(cast(order_id as varchar), '') as varchar)) as hash_id,
final.* 
from final

-----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."discount_application_at")
  