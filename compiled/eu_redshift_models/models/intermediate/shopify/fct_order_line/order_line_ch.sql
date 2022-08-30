
      
   with 
   
    order_line_raw as (
    
    select
    
	   
   
		    order_line._fivetran_synced,
		    date(order_line._fivetran_synced) as creation_date,
		    order_line.destination_location_address_1,
		    order_line.destination_location_address_2,
		    order_line.destination_location_city,
		    order_line.destination_location_country_code,
		    order_line.destination_location_id,
		    order_line.destination_location_name,
		    order_line.destination_location_province_code,
		    order_line.destination_location_zip,
		    order_line.fulfillable_quantity,
		    order_line.fulfillment_service,
		    order_line.fulfillment_status,
		    order_line.gift_card,
		    order_line.grams,
		    order_line.id,
		    order_line.index,
		    order_line.name,
		    order_line.order_id,
		    order_line.origin_location_address_1,
		    order_line.origin_location_address_2,
		    order_line.origin_location_city,
		    order_line.origin_location_country_code,
		    order_line.origin_location_id,
		    order_line.origin_location_name,
		    order_line.origin_location_province_code,
		    order_line.origin_location_zip,
		    order_line.pre_tax_price,									
		    order_line.pre_tax_price AS pre_tax_price_chf,									
		    NULL::double precision AS pre_tax_price_gbp,								 
		    NULL::double precision AS pre_tax_price_sek,
			order_line.pre_tax_price_set,
		    order_line.price,
		    order_line.price AS price_chf,							
		    NULL::double precision AS price_gbp,
			NULL::double precision AS price_sek,						 
		    order_line.price_set,
		    order_line.product_exists,
		    order_line.product_id,
		    order_line.properties,
		    order_line.quantity,
		    order_line.requires_shipping,
		    order_line.sku,
		    order_line.tax_code,
		    order_line.taxable,
		    order_line.title,
		    order_line.total_discount,									 
		    order_line.total_discount AS total_discount_chf,
		    NULL::double precision AS total_discount_gbp,
		    NULL::double precision AS total_discount_sek,
			order_line.total_discount_set,
		    order_line.variant_id,
		    order_line.variant_inventory_management,
		    order_line.variant_title,
		    order_line.vendor,
		   'CH'::text AS shopify_shop,
		   'CHF'::text as currency_abbreviation
		    
	 FROM "airup_eu_dwh"."shopify_ch"."order_line"
       

   ),
    
    global_curr_chf as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur

    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
       where currency_abbreviation = 'CHF'
    
    ),
  
  order_line_enriched as (
  
   select
   
            order_line_raw._fivetran_synced,
		    order_line_raw.creation_date,
		    order_line_raw.destination_location_address_1,
		    order_line_raw.destination_location_address_2,
		    order_line_raw.destination_location_city,
		    order_line_raw.destination_location_country_code,
		    order_line_raw.destination_location_id,
		    order_line_raw.destination_location_name,
		    order_line_raw.destination_location_province_code,
		    order_line_raw.destination_location_zip,
		    order_line_raw.fulfillable_quantity,
		    order_line_raw.fulfillment_service,
		    order_line_raw.fulfillment_status,
		    order_line_raw.gift_card,
		    order_line_raw.grams,
		    order_line_raw.id,
		    order_line_raw.index,
		    order_line_raw.name,
		    order_line_raw.order_id,
		    order_line_raw.origin_location_address_1,
		    order_line_raw.origin_location_address_2,
		    order_line_raw.origin_location_city,
		    order_line_raw.origin_location_country_code,
		    order_line_raw.origin_location_id,
		    order_line_raw.origin_location_name,
		    order_line_raw.origin_location_province_code,
		    order_line_raw.origin_location_zip,
		    (order_line_raw.pre_tax_price / case when global_curr_chf.conversion_rate_eur is not null then global_curr_chf.conversion_rate_eur else 1.0986 end)::numeric(10,3)::double precision as pre_tax_price,
									 
		    order_line_raw.pre_tax_price_chf,
		    order_line_raw.pre_tax_price_gbp,
		    order_line_raw.pre_tax_price_sek,
			order_line_raw.pre_tax_price_set,
		    (order_line_raw.price / case when global_curr_chf.conversion_rate_eur is not null then global_curr_chf.conversion_rate_eur else 1.0986 end)::numeric(10,3)::double precision as price,
							 
		    order_line_raw.price_chf,
		    order_line_raw.price_gbp,
		    order_line_raw.price_sek,
			order_line_raw.price_set,
		    order_line_raw.product_exists,
		    order_line_raw.product_id,
		    order_line_raw.properties,
		    order_line_raw.quantity,
		    order_line_raw.requires_shipping,
		    order_line_raw.sku,
		    order_line_raw.tax_code,
		    order_line_raw.taxable,
		    order_line_raw.title,
		    (order_line_raw.total_discount / case when global_curr_chf.conversion_rate_eur is not null then global_curr_chf.conversion_rate_eur else 1.0986 end)::numeric(10,3)::double precision as total_discount,
									  
		    order_line_raw.total_discount_chf,
		    order_line_raw.total_discount_gbp,
		    order_line_raw.total_discount_sek,
			order_line_raw.total_discount_set,
		    order_line_raw.variant_id,
		    order_line_raw.variant_inventory_management,
		    order_line_raw.variant_title,
		    order_line_raw.vendor,
		    order_line_raw.shopify_shop,
		   global_curr_chf.currency_abbreviation,
	       global_curr_chf.conversion_rate_eur
		    
    from order_line_raw
    left join global_curr_chf using (currency_abbreviation, creation_date)
  
  )
  select * from order_line_enriched
 
  -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."order_line_ch")
  