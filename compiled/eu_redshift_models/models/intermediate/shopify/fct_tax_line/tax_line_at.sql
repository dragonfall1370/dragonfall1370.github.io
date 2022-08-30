 
 
 with 
   
    tax_line_raw as (
    
   select
	    tax_line._fivetran_synced,
	    date(tax_line._fivetran_synced) as creation_date,
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
	    'AT'::text AS shopify_shop,
	    'EUR'::text as currency_abbreviation
   FROM "airup_eu_dwh"."shopify_at"."tax_line"
    
   ),
    
    global_curr_eur as (
    
    select 
       date(creation_datetime) as creation_date,
       currency_abbreviation, 
       conversion_rate_eur
       
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" 
       where currency_abbreviation = 'EUR'
    
    ),
  
  tax_line_enriched as (
  
   select
        tax_line_raw._fivetran_synced,
	    tax_line_raw.creation_date,
	    tax_line_raw.index,
	    tax_line_raw.order_line_id,
	    (tax_line_raw.price / case when global_curr_eur.conversion_rate_eur is not null then global_curr_eur.conversion_rate_eur else 1 end)::numeric(10,3)::double precision as price ,    							   
	    tax_line_raw.price_chf,
	    tax_line_raw.price_gbp AS price_gbp,
		tax_line_raw.price_sek AS price_sek,
	    tax_line_raw.price_set,
	    (tax_line_raw.rate / case when global_curr_eur.conversion_rate_eur is not null then global_curr_eur.conversion_rate_eur else 1 end)::numeric(10,3)::double precision as rate, 
	    tax_line_raw.rate_chf,
	    tax_line_raw.rate_gbp,
		tax_line_raw.rate_sek,
	    tax_line_raw.title,
	    tax_line_raw.shopify_shop,
	    global_curr_eur.currency_abbreviation,
	    global_curr_eur.conversion_rate_eur
	    ---global_curr_gbp.creation_date
    from tax_line_raw
    left join global_curr_eur using (currency_abbreviation, creation_date)
  
  )
  select * from tax_line_enriched

    -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."tax_line_at")
  