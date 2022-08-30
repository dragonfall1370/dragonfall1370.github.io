
       
with 
   
  order_adjustment_raw as (
    
 select
    
    order_adjustment._fivetran_synced,
    date(order_adjustment._fivetran_synced) as creation_date,
    order_adjustment.amount,
    order_adjustment.amount AS amount_chf,
    NULL::double precision AS amount_gbp,
 	  NULL::double precision AS amount_sek,
    order_adjustment.amount_set,
    order_adjustment.id,
    order_adjustment.kind,
    order_adjustment.order_id,
    order_adjustment.reason,
    order_adjustment.refund_id,
    order_adjustment.tax_amount,
    order_adjustment.tax_amount AS tax_amount_chf,
    NULL::double precision AS tax_amount_gbp,
	  NULL::double precision AS tax_amount_sek,
    order_adjustment.tax_amount_set,
    'CH'::text AS shopify_shop,
    'CHF'::text as currency_abbreviation
    
   FROM "airup_eu_dwh"."shopify_ch"."order_adjustment"
   
   ),
    
    global_curr_chf as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur

     FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
     where currency_abbreviation = 'CHF'
    
    ),
  
  order_adjustment_enriched as (
  
   select
   
        order_adjustment_raw._fivetran_synced,
	    order_adjustment_raw.creation_date,
	   (order_adjustment_raw.amount / case when global_curr_chf.conversion_rate_eur is not null then global_curr_chf.conversion_rate_eur else 1.0986 end)::numeric(10,3)::double precision as amount,
	    order_adjustment_raw.amount_chf,
	    order_adjustment_raw.amount_gbp,
		order_adjustment_raw.amount_sek,						  
	    order_adjustment_raw.amount_set,
	    order_adjustment_raw.id,
	    order_adjustment_raw.kind,
	    order_adjustment_raw.order_id,
	    order_adjustment_raw.reason,
	    order_adjustment_raw.refund_id,
	   (order_adjustment_raw.tax_amount / case when global_curr_chf.conversion_rate_eur is not null then global_curr_chf.conversion_rate_eur else 1.0986 end)::numeric(10,3)::double precision as tax_amount,
	    order_adjustment_raw.tax_amount_chf,
	    order_adjustment_raw.tax_amount_gbp,
		order_adjustment_raw.tax_amount_sek,							  
	    order_adjustment_raw.tax_amount_set,
	    order_adjustment_raw.shopify_shop,
	    global_curr_chf.currency_abbreviation,
	    global_curr_chf.conversion_rate_eur
	    ---global_curr_gbp.creation_date
    from order_adjustment_raw
    left join global_curr_chf using (currency_abbreviation, creation_date)
  
  )
  select * from order_adjustment_enriched

     -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."order_adjustment_ch")
  