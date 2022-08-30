 
 
 
   
 WITH tax_line_raw AS (
	
		 
         SELECT tax_line._fivetran_synced,
            date(tax_line._fivetran_synced) AS creation_date,
            tax_line.index,
            tax_line.order_line_id,
            tax_line.price,
            NULL::double precision AS price_chf,
            NULL::double precision AS price_gbp,
			NULL::double precision AS price_sek,						  
            tax_line.price_set,
            tax_line.rate,
            NULL::double precision AS rate_chf,
            NULL::double precision AS rate_gbp,
			NULL::double precision AS rate_sek,						 
            tax_line.title,
            'IT'::text AS shopify_shop,
            'EUR'::text AS currency_abbreviation
           FROM "airup_eu_dwh"."shopify_it"."tax_line"
           
	 
	
        ), global_curr_eur AS (
	
         select 
            date(creation_datetime) as creation_date,
            currency_abbreviation, 
            conversion_rate_eur

            FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" 
            where currency_abbreviation = 'EUR'

        ),
        
         tax_line_enriched AS (
  
		 
         SELECT tax_line_raw._fivetran_synced,
            tax_line_raw.creation_date,
            tax_line_raw.index,
            tax_line_raw.order_line_id,
            (tax_line_raw.price /
                CASE
                    WHEN global_curr_eur.conversion_rate_eur IS NOT NULL THEN global_curr_eur.conversion_rate_eur
                    ELSE 1::double precision
                END)::numeric(10,3)::double precision AS price,
            tax_line_raw.price_chf,
            tax_line_raw.price_gbp,
			tax_line_raw.price_sek AS price_sek,
            tax_line_raw.price_set,
            (tax_line_raw.rate /
                CASE
                    WHEN global_curr_eur.conversion_rate_eur IS NOT NULL THEN global_curr_eur.conversion_rate_eur
                    ELSE 1::double precision
                END)::numeric(10,3)::double precision AS rate,
            tax_line_raw.rate_chf,
            tax_line_raw.rate_gbp,
			tax_line_raw.rate_sek,
            tax_line_raw.title,
            tax_line_raw.shopify_shop,
            global_curr_eur.currency_abbreviation,
            global_curr_eur.conversion_rate_eur
									 
           FROM tax_line_raw
             LEFT JOIN global_curr_eur USING (currency_abbreviation, creation_date)
  
        )
 SELECT * FROM tax_line_enriched

     -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."tax_line_it")
  