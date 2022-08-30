
    
with 
   
    order_line_refund_raw as (
    
 select
   
    order_line_refund._fivetran_synced,
    date(order_line_refund._fivetran_synced) as creation_date,
    order_line_refund.id,
    order_line_refund.location_id,
    order_line_refund.order_line_id,
    order_line_refund.quantity,
    order_line_refund.refund_id,
    order_line_refund.restock_type,
    order_line_refund.subtotal,
    NULL::double precision AS subtotal_chf,
    NULL::double precision AS subtotal_gbp,
	NULL::double precision AS subtotal_sek,
    order_line_refund.subtotal_set,
    order_line_refund.total_tax,
    NULL::double precision AS total_tax_chf,
    NULL::double precision AS total_tax_gbp,
	NULL::double precision AS total_tax_sek,
    order_line_refund.total_tax_set,
   'AT'::text AS shopify_shop,
   'EUR'::text as currency_abbreviation
   
   FROM "airup_eu_dwh"."shopify_at"."order_line_refund"
   

   ),
    
    global_curr_eur as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
       where currency_abbreviation = 'EUR'
    
    ),
  
  order_line_refund_enriched as (
  
   select   
    order_line_refund_raw._fivetran_synced,
    order_line_refund_raw.creation_date,
    order_line_refund_raw.id,
    order_line_refund_raw.location_id,
    order_line_refund_raw.order_line_id,
    order_line_refund_raw.quantity,
    order_line_refund_raw.refund_id,
    order_line_refund_raw.restock_type,
    order_line_refund_raw.subtotal,
    order_line_refund_raw.subtotal_chf,
    order_line_refund_raw.subtotal_gbp,
	order_line_refund_raw.subtotal_sek,
    order_line_refund_raw.subtotal_set,
    order_line_refund_raw.total_tax,
    order_line_refund_raw.total_tax_chf,
    order_line_refund_raw.total_tax_gbp,
	order_line_refund_raw.total_tax_sek,
    order_line_refund_raw.total_tax_set,
    order_line_refund_raw.shopify_shop,
    global_curr_eur.currency_abbreviation,
	global_curr_eur.conversion_rate_eur
 
   from order_line_refund_raw
    left join global_curr_eur using (currency_abbreviation, creation_date)
  
  )
  select * from order_line_refund_enriched

   -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."order_line_refund_at")
  