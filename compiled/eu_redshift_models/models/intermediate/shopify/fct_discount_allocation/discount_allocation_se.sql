  
   with 
   
    discount_allocation_raw as (
    
    SELECT 
    
    discount_allocation._fivetran_synced,
    date(discount_allocation._fivetran_synced) as creation_date,
    discount_allocation.amount,
    discount_allocation.amount AS amount_sek,
    NULL::double precision AS amount_chf,
    NULL::double precision AS amount_gbp,
    discount_allocation.amount_set_presentment_money_amount,
    discount_allocation.amount_set_presentment_money_amount AS amount_set_presentment_money_amount_sek,
    NULL::double precision AS amount_set_presentment_money_amount_chf,
	NULL::double precision AS amount_set_presentment_money_amount_gbp,
    discount_allocation.amount_set_presentment_money_currency_code,
    discount_allocation.amount_set_shop_money_amount,
    discount_allocation.amount_set_shop_money_amount AS amount_set_shop_money_amount_sek,
    NULL::double precision AS amount_set_shop_money_amount_chf,
	NULL::double precision AS amount_set_shop_money_amount_gbp,
    discount_allocation.amount_set_shop_money_currency_code,
    discount_allocation.discount_application_index,
    discount_allocation.index,
    discount_allocation.order_line_id,
    'SE'::text AS shopify_shop,
    'SEK'::text as currency_abbreviation
    
  FROM "airup_eu_dwh"."shopify_se"."discount_allocation"
   
   
   ),
    
    global_curr_sek as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
       
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
       where currency_abbreviation = 'SEK'
    
    ),
  
  discount_allocation_enriched as (
  
   select
   
        discount_allocation_raw._fivetran_synced,
	    discount_allocation_raw.creation_date,
	    (discount_allocation_raw.amount / case when global_curr_sek.conversion_rate_eur is not null then global_curr_sek.conversion_rate_eur else 10.69 end)::numeric(10,3)::double precision as amount,
	    discount_allocation_raw.amount_sek,
        discount_allocation_raw.amount_chf,
	    discount_allocation_raw.amount_gbp,
	    (discount_allocation_raw.amount_set_presentment_money_amount / case when global_curr_sek.conversion_rate_eur is not null then global_curr_sek.conversion_rate_eur else 10.69 end)::numeric(10,3)::double precision as amount_set_presentment_money_amount,
	    discount_allocation_raw.amount_set_presentment_money_amount_sek,
        discount_allocation_raw.amount_set_presentment_money_amount_chf,
	    discount_allocation_raw.amount_set_presentment_money_amount_gbp,
	    discount_allocation_raw.amount_set_presentment_money_currency_code,
	    (discount_allocation_raw.amount_set_shop_money_amount / case when global_curr_sek.conversion_rate_eur is not null then global_curr_sek.conversion_rate_eur else 10.69 end)::numeric(10,3)::double precision as amount_set_shop_money_amount,
	    discount_allocation_raw.amount_set_shop_money_amount_sek,
        discount_allocation_raw.amount_set_shop_money_amount_chf,
	    discount_allocation_raw.amount_set_shop_money_amount_gbp,
	    discount_allocation_raw.amount_set_shop_money_currency_code,
	    discount_allocation_raw.discount_application_index,
	    discount_allocation_raw.index,
	    discount_allocation_raw.order_line_id,
	    discount_allocation_raw.shopify_shop,
	    global_curr_sek.currency_abbreviation,
	    global_curr_sek.conversion_rate_eur
	    ---global_curr_gbp.creation_date
    from discount_allocation_raw
    left join global_curr_sek using (currency_abbreviation, creation_date)
  
  )
  select * from discount_allocation_enriched
  
  -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."discount_allocation_se")
  