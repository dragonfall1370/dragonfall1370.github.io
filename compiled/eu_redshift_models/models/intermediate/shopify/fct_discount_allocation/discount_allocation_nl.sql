
   with 
   
    discount_allocation_raw as (
    
   select
   
        discount_allocation._fivetran_synced,
        date(discount_allocation._fivetran_synced) as creation_date,
	    discount_allocation.amount,
	    NULL::double precision AS amount_chf,
	    NULL::double precision AS amount_gbp,
		NULL::double precision AS amount_sek,
	    discount_allocation.amount_set_presentment_money_amount,
	    NULL::double precision AS amount_set_presentment_money_amount_chf,
	    NULL::double precision AS amount_set_presentment_money_amount_gbp,
		NULL::double precision AS amount_set_presentment_money_amount_sek,
	    discount_allocation.amount_set_presentment_money_currency_code,
	    discount_allocation.amount_set_shop_money_amount,
	    NULL::double precision AS amount_set_shop_money_amount_chf,
	    NULL::double precision AS amount_set_shop_money_amount_gbp,
		NULL::double precision AS amount_set_shop_money_amount_sek,
	    discount_allocation.amount_set_shop_money_currency_code,
	    discount_allocation.discount_application_index,
	    discount_allocation.index,
	    discount_allocation.order_line_id,
	    'NL'::text AS shopify_shop,
   	    'EUR'::text as currency_abbreviation
   	    
   FROM "airup_eu_dwh"."shopify_nl"."discount_allocation"
    

   ),
    
    global_curr_eur as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
       
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
    WHERE currency_abbreviation = 'EUR'
    
    ),
  
  discount_allocation_enriched as (
  
   select
   
        discount_allocation_raw._fivetran_synced,
        discount_allocation_raw.creation_date,
	    discount_allocation_raw.amount,
	    discount_allocation_raw.amount_chf,
	    discount_allocation_raw.amount_gbp,
		discount_allocation_raw.amount_sek,
	    discount_allocation_raw.amount_set_presentment_money_amount,
	    discount_allocation_raw.amount_set_presentment_money_amount_chf,
	    discount_allocation_raw.amount_set_presentment_money_amount_gbp,
		discount_allocation_raw.amount_set_presentment_money_amount_sek,
	    discount_allocation_raw.amount_set_presentment_money_currency_code,
	    discount_allocation_raw.amount_set_shop_money_amount,
	    discount_allocation_raw.amount_set_shop_money_amount_chf,
	    discount_allocation_raw.amount_set_shop_money_amount_gbp,
		discount_allocation_raw.amount_set_shop_money_amount_sek,
	    discount_allocation_raw.amount_set_shop_money_currency_code,
	    discount_allocation_raw.discount_application_index,
	    discount_allocation_raw.index,
	    discount_allocation_raw.order_line_id,
	    discount_allocation_raw.shopify_shop,
	    global_curr_eur.currency_abbreviation,
	    global_curr_eur.conversion_rate_eur
	    ---global_curr_gbp.creation_date
    from discount_allocation_raw
    left join global_curr_eur using (currency_abbreviation, creation_date)
  
  )
  select * from discount_allocation_enriched
  
    -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."discount_allocation_nl")
  