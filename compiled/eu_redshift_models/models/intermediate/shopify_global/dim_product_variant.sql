 
 

SELECT 
     _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
FROM "airup_eu_dwh"."shopify_global"."product_variant_de"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'Base')
  
   
UNION all

SELECT 
     _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		   compare_at_price_sek,
		   null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
FROM "airup_eu_dwh"."shopify_global"."product_variant_at"
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'AT')
  
   
UNION all

 SELECT 
    _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		   compare_at_price_sek,
		   null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
 FROM "airup_eu_dwh"."shopify_global"."product_variant_ch"
             -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'CH')
  

      
UNION all

 SELECT 
     _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
 FROM "airup_eu_dwh"."shopify_global"."product_variant_fr"
             -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'FR')
  

UNION all

 SELECT 
      _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
  FROM "airup_eu_dwh"."shopify_global"."product_variant_it"
              -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'IT')
  
   
UNION all

 SELECT 
       _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."product_variant_nl"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'NL')
  

 UNION all  
 SELECT 
      _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."product_variant_uk"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'UK')
  

 UNION all  
 SELECT 
     _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 null as compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 null as price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."product_variant_se"
               -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'SE')
  


 UNION all  
 SELECT 
     _fivetran_synced,
	     creation_date,
	     barcode,
	     compare_at_price,  
	     compare_at_price_chf,
	     compare_at_price_gbp,
		 compare_at_price_sek,
		 compare_at_price_usd,
	     created_at,
	     fulfillment_service,
	     grams,
	     id,
	     image_id,
	     inventory_item_id,
	     inventory_management,
	     inventory_policy,
	     inventory_quantity,
	     old_inventory_quantity,
	     option_1,
	     option_2,
	     option_3,
	     "position",
	     price,  
	     price_chf,
	     price_gbp,
		 price_sek,
		 price_usd,
	     product_id,
	     requires_shipping,
	     sku,
	     tax_code,
	     taxable,
	     title,
	     updated_at,
	     weight,
	     weight_unit,
	     shopify_shop,
	    currency_abbreviation,
	    conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."product_variant_us"


            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."dim_product_variant" where 1=1 and shopify_shop = 'US')
  