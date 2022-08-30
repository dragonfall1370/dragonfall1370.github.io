   
   with 
   
    order_shipping_line_raw as (
    
  SELECT 
  
    order_shipping_line._fivetran_synced,
    date(order_shipping_line._fivetran_synced) as creation_date,
    order_shipping_line.carrier_identifier,
    order_shipping_line.code,
    order_shipping_line.delivery_category,
    order_shipping_line.discounted_price,
    NULL::double precision AS discounted_price_chf,												
    order_shipping_line.discounted_price AS discounted_price_gbp,
	  NULL::double precision AS discounted_price_sek,
    order_shipping_line.discounted_price_set,
    order_shipping_line.id,
    order_shipping_line.order_id,
    order_shipping_line.phone,
    order_shipping_line.price,
    NULL::double precision AS price_chf,
    order_shipping_line.price AS price_gbp,
	  NULL::double precision AS price_sek,								  
    order_shipping_line.price_set,
    order_shipping_line.requested_fulfillment_service_id,
    order_shipping_line.source,
    order_shipping_line.title,
    'UK'::text AS shopify_shop,
    'GBP'::text as currency_abbreviation
	 														 
   FROM "airup_eu_dwh"."shopify_uk"."order_shipping_line"
   
   ),
    
    global_curr_gbp as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur

     FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" 
       where currency_abbreviation = 'GBP'
    
    ),
  
  order_shipping_line_enriched as (
  
   select
   
    order_shipping_line_raw._fivetran_synced,
    order_shipping_line_raw.creation_date,
    order_shipping_line_raw.carrier_identifier,
    order_shipping_line_raw.code,
    order_shipping_line_raw.delivery_category,
    (order_shipping_line_raw.discounted_price / case when global_curr_gbp.conversion_rate_eur is not null then global_curr_gbp.conversion_rate_eur else 0.863 end)::numeric(10,3)::double precision as discounted_price,											  
    order_shipping_line_raw.discounted_price_chf,
    order_shipping_line_raw.discounted_price_gbp,
	  order_shipping_line_raw.discounted_price_sek,
    order_shipping_line_raw.discounted_price_set,
    order_shipping_line_raw.id,
    order_shipping_line_raw.order_id,
    order_shipping_line_raw.phone,
    (order_shipping_line_raw.price / case when global_curr_gbp.conversion_rate_eur is not null then global_curr_gbp.conversion_rate_eur else 0.863 end)::numeric(10,3)::double precision as price,									   
    order_shipping_line_raw.price_chf,
    order_shipping_line_raw.price_gbp,
	  order_shipping_line_raw.price_sek,
    order_shipping_line_raw.price_set,
    order_shipping_line_raw.requested_fulfillment_service_id,
    order_shipping_line_raw.source,
    order_shipping_line_raw.title,
    order_shipping_line_raw.shopify_shop,
	global_curr_gbp.currency_abbreviation,
	global_curr_gbp.conversion_rate_eur
	    ---global_curr_gbp.creation_date
    from order_shipping_line_raw
    left join global_curr_gbp using (currency_abbreviation, creation_date)
  
  )
  select * from order_shipping_line_enriched

  -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."order_shipping_line_uk")
  