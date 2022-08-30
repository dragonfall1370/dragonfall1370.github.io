
    
   
		
   
WITH order_adjustment_raw AS (
	
		 
   
         SELECT order_adjustment._fivetran_synced,
            date(order_adjustment._fivetran_synced) AS creation_date,
            order_adjustment.amount,
            NULL::double precision AS amount_chf,
            NULL::double precision AS amount_gbp,
			NULL::double precision AS amount_sek,						   
            order_adjustment.amount_set,
            order_adjustment.id,
            order_adjustment.kind,
            order_adjustment.order_id,
            order_adjustment.reason,
            order_adjustment.refund_id,
            order_adjustment.tax_amount,
            NULL::double precision AS tax_amount_chf,
            NULL::double precision AS tax_amount_gbp,
			NULL::double precision AS tax_amount_sek,							   
            order_adjustment.tax_amount_set,
            'IT'::text AS shopify_shop,
            'EUR'::text AS currency_abbreviation

           FROM "airup_eu_dwh"."shopify_it"."order_adjustment"

        ), global_curr_eur AS (
          select 
            date(creation_datetime) as creation_date,
            currency_abbreviation, 
            conversion_rate_eur

          FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
          where currency_abbreviation = 'EUR'

        ), 
		

		order_adjustment_enriched AS (
  
		 
		 
         SELECT order_adjustment_raw._fivetran_synced,
            order_adjustment_raw.creation_date,
            order_adjustment_raw.amount,
            order_adjustment_raw.amount_chf,
            order_adjustment_raw.amount_gbp,
			order_adjustment_raw.amount_sek,
            order_adjustment_raw.amount_set,					 
            order_adjustment_raw.id,
            order_adjustment_raw.kind,
            order_adjustment_raw.order_id,
            order_adjustment_raw.reason,
            order_adjustment_raw.refund_id,
            order_adjustment_raw.tax_amount,
            order_adjustment_raw.tax_amount_chf,
            order_adjustment_raw.tax_amount_gbp,
			order_adjustment_raw.tax_amount_sek,
            order_adjustment_raw.tax_amount_set,						 
            order_adjustment_raw.shopify_shop,
            order_adjustment_raw.currency_abbreviation,
            global_curr_eur.conversion_rate_eur
									 
           FROM order_adjustment_raw
             LEFT JOIN global_curr_eur USING (currency_abbreviation, creation_date)
  
        )
 SELECT *
   FROM order_adjustment_enriched

  -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."order_adjustment_it")
  