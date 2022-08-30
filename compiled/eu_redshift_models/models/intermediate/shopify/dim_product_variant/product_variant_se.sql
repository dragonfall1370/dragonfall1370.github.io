   
   
   with 
   
    product_variant_raw as (
    
  select
  
    product_variant._fivetran_synced,
    date(product_variant._fivetran_synced) as creation_date,
    product_variant.barcode,
    product_variant.compare_at_price,
    product_variant.compare_at_price AS compare_at_price_sek,
    NULL::double precision AS compare_at_price_chf,
	NULL::double precision AS compare_at_price_gbp,  --add a compare price for swedwisk krona																					   
    product_variant.created_at,
    product_variant.fulfillment_service,
    product_variant.grams,
    product_variant.id,
    product_variant.image_id,
    product_variant.inventory_item_id,
    product_variant.inventory_management,
    product_variant.inventory_policy,
    product_variant.inventory_quantity,
    product_variant.old_inventory_quantity,
    product_variant.option_1,
    product_variant.option_2,
    product_variant.option_3,
    product_variant."position",
    product_variant.price,
    product_variant.price AS price_sek,
    NULL::double precision AS price_chf,
	NULL::double precision AS price_gbp, 									  
    product_variant.product_id,
    product_variant.requires_shipping,
    product_variant.sku,
    product_variant.tax_code,
    product_variant.taxable,
    product_variant.title,
    product_variant.updated_at,
    product_variant.weight,
    product_variant.weight_unit,
    'SE'::text AS shopify_shop,
    'SEK'::text as currency_abbreviation
    
  FROM "airup_eu_dwh"."shopify_se"."product_variant"
    

   ),

													  
    
    global_curr_sek as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
       
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
       where currency_abbreviation = 'SEK'
    
    ),
  
														   
  product_variant_enriched as (
  
  select
   
    product_variant_raw._fivetran_synced,
    product_variant_raw.creation_date,
    product_variant_raw.barcode,
    (product_variant_raw.compare_at_price / case when global_curr_sek.conversion_rate_eur is not null then global_curr_sek.conversion_rate_eur else 10.69 end)::numeric(10,3)::double precision as compare_at_price,										  
    product_variant_raw.compare_at_price_sek,
    product_variant_raw.compare_at_price_chf,
    product_variant_raw.compare_at_price_gbp,
    product_variant_raw.created_at,
    product_variant_raw.fulfillment_service,
    product_variant_raw.grams,
    product_variant_raw.id,
    product_variant_raw.image_id,
    product_variant_raw.inventory_item_id,
    product_variant_raw.inventory_management,
    product_variant_raw.inventory_policy,
    product_variant_raw.inventory_quantity,
    product_variant_raw.old_inventory_quantity,
    product_variant_raw.option_1,
    product_variant_raw.option_2,
    product_variant_raw.option_3,
    product_variant_raw."position",
   (product_variant_raw.price / case when global_curr_sek.conversion_rate_eur is not null then global_curr_sek.conversion_rate_eur else 10.69 end)::numeric(10,3)::double precision as price,							   
    product_variant_raw.price_sek,
    product_variant_raw.price_chf,
    product_variant_raw.price_gbp,
    product_variant_raw.product_id,
    product_variant_raw.requires_shipping,
    product_variant_raw.sku,
    product_variant_raw.tax_code,
    product_variant_raw.taxable,
    product_variant_raw.title,
    product_variant_raw.updated_at,
    product_variant_raw.weight,
    product_variant_raw.weight_unit,
    product_variant_raw.shopify_shop,
    global_curr_sek.currency_abbreviation,
	global_curr_sek.conversion_rate_eur	      
	
    from product_variant_raw
    left join global_curr_sek using (currency_abbreviation, creation_date)
  
  )
  select * from product_variant_enriched

     -----incremental table macro---
    
   where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."product_variant_se")
  