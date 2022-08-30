 
 
WITH order_shipping_tax_line_raw AS (
	
         SELECT order_shipping_tax_line._fivetran_synced,
            date(order_shipping_tax_line._fivetran_synced) AS creation_date,
            order_shipping_tax_line.index,
            order_shipping_tax_line.order_shipping_line_id,
            order_shipping_tax_line.price,
            NULL::double precision AS price_chf,
            NULL::double precision AS price_gbp,
			NULL::double precision AS price_sek,						 
            order_shipping_tax_line.price_set,
            order_shipping_tax_line.rate,
            NULL::double precision AS rate_chf,
            NULL::double precision AS rate_gbp,
			NULL::double precision AS rate_sek,						
            order_shipping_tax_line.title,
            'IT'::text AS shopify_shop,
            'EUR'::text AS currency_abbreviation
															
        FROM "airup_eu_dwh"."shopify_it"."order_shipping_tax_line"		
		), 
		
		global_curr_eur AS (					
	
        select 
            date(creation_datetime) as creation_date,
            currency_abbreviation, 
            conversion_rate_eur
        FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
            where currency_abbreviation = 'EUR'
 
        ), 
        
        order_shipping_tax_line_enriched AS (
  
	   SELECT 
	   
	        order_shipping_tax_line_raw._fivetran_synced,
            order_shipping_tax_line_raw.creation_date,
            order_shipping_tax_line_raw.index,
            order_shipping_tax_line_raw.order_shipping_line_id,
            (order_shipping_tax_line_raw.price /
                CASE
                    WHEN global_curr_eur.conversion_rate_eur IS NOT NULL THEN global_curr_eur.conversion_rate_eur
                    ELSE 1::double precision
                END)::numeric(10,3)::double precision AS price,
            order_shipping_tax_line_raw.price_chf,
            order_shipping_tax_line_raw.price_gbp,
			order_shipping_tax_line_raw.price_sek,							 
            order_shipping_tax_line_raw.price_set,
            (order_shipping_tax_line_raw.rate /
                CASE
                    WHEN global_curr_eur.conversion_rate_eur IS NOT NULL THEN global_curr_eur.conversion_rate_eur
                    ELSE 1::double precision
                END)::numeric(10,3)::double precision AS rate,
            order_shipping_tax_line_raw.rate_chf,
            order_shipping_tax_line_raw.rate_gbp,
			order_shipping_tax_line_raw.rate_sek,							
            order_shipping_tax_line_raw.title,
            order_shipping_tax_line_raw.shopify_shop,
            global_curr_eur.currency_abbreviation,
            global_curr_eur.conversion_rate_eur
   
           FROM order_shipping_tax_line_raw
             LEFT JOIN global_curr_eur USING (currency_abbreviation, creation_date)
        )
 SELECT *
   FROM order_shipping_tax_line_enriched

     
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."order_shipping_tax_line_it")
  