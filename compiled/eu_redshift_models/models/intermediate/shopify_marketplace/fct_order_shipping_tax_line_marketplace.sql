 
 
 with 
   
    order_shipping_tax_line_raw as (
    
   select
   
    order_shipping_tax_line._fivetran_synced,
    date(order_shipping_tax_line._fivetran_synced) as creation_date,
    order_shipping_tax_line.index,
    order_shipping_tax_line.order_shipping_line_id,
    order_shipping_tax_line.price,
    NULL::double precision AS price_chf,
    NULL::double precision AS price_gbp,
    order_shipping_tax_line.price_set,
    order_shipping_tax_line.rate,
    NULL::double precision AS rate_chf,
    NULL::double precision AS rate_gbp,
    order_shipping_tax_line.title,
    'Base'::text AS shopify_shop,
    'EUR'::text as currency_abbreviation
    
   FROM "airup_eu_dwh"."shopify_marketplace"."order_shipping_tax_line"


   ),
    
    global_curr_eur as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
       where currency_abbreviation = 'EUR'
 
    ),
  
  order_shipping_tax_line_enriched as (
  
 select
   
   order_shipping_tax_line_raw._fivetran_synced,
   order_shipping_tax_line_raw.creation_date,
   order_shipping_tax_line_raw.index,
   order_shipping_tax_line_raw.order_shipping_line_id,
   (order_shipping_tax_line_raw.price / case when global_curr_eur.conversion_rate_eur is not null then global_curr_eur.conversion_rate_eur else 1 end)::numeric(10,3)::double precision as price,
   order_shipping_tax_line_raw.price_chf,
   order_shipping_tax_line_raw.price_gbp,
   order_shipping_tax_line_raw.price_set,
   (order_shipping_tax_line_raw.rate / case when global_curr_eur.conversion_rate_eur is not null then global_curr_eur.conversion_rate_eur else 1 end)::numeric(10,3)::double precision as rate,
   order_shipping_tax_line_raw.rate_chf,
   order_shipping_tax_line_raw.rate_gbp,
   order_shipping_tax_line_raw.title,
   order_shipping_tax_line_raw.shopify_shop,
   global_curr_eur.currency_abbreviation,
   global_curr_eur.conversion_rate_eur
   
from order_shipping_tax_line_raw
left join global_curr_eur using (currency_abbreviation, creation_date)
      
  
  )
  select * from order_shipping_tax_line_enriched

  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_marketplace"."fct_order_shipping_tax_line_marketplace")
  