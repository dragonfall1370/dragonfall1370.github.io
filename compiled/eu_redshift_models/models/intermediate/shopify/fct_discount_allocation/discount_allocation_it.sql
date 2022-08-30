
WITH discount_allocation_raw AS (
         SELECT discount_allocation._fivetran_synced,
            date(discount_allocation._fivetran_synced) AS creation_date,
            discount_allocation.amount,
            NULL::double precision AS amount_chf,
            NULL::double precision AS amount_gbp,
			NULL::double precision AS amount_sek,
            discount_allocation.amount_set_presentment_money_amount,
            NULL::double precision AS amount_set_presentment_money_amount_chf,
            NULL::double precision AS amount_set_presentment_money_amount_gbp,
			NULL::double precision AS amount_set_presentment_money_amount_sek,
            discount_allocation.amount_set_presentment_money_currency_code,
            discount_allocation.amount_set_shop_money_amount,
            NULL::double precision AS amount_set_shop_money_amount_chf,
            NULL::double precision AS amount_set_shop_money_amount_gbp,
			NULL::double precision AS amount_set_shop_money_amount_sek,
            discount_allocation.amount_set_shop_money_currency_code,
            discount_allocation.discount_application_index,
            discount_allocation.index,
            discount_allocation.order_line_id,
            'IT'::text AS shopify_shop,
            'EUR'::text AS currency_abbreviation

           FROM "airup_eu_dwh"."shopify_it"."discount_allocation"

        ), global_curr_eur AS (
         SELECT date(creation_datetime) AS creation_date,
                 currency_abbreviation,
                  conversion_rate_eur

          FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
          WHERE currency_abbreviation::text = 'EUR'::text

        ), discount_allocation_enriched AS (
         SELECT discount_allocation_raw._fivetran_synced,
            discount_allocation_raw.creation_date,
            discount_allocation_raw.amount,
            discount_allocation_raw.amount_chf,
            discount_allocation_raw.amount_gbp,
			discount_allocation_raw.amount_sek,
            discount_allocation_raw.amount_set_presentment_money_amount,
            discount_allocation_raw.amount_set_presentment_money_amount_chf,
            discount_allocation_raw.amount_set_presentment_money_amount_gbp,
			discount_allocation_raw.amount_set_presentment_money_amount_sek,
            discount_allocation_raw.amount_set_presentment_money_currency_code,
            discount_allocation_raw.amount_set_shop_money_amount,
            discount_allocation_raw.amount_set_shop_money_amount_chf,
            discount_allocation_raw.amount_set_shop_money_amount_gbp,
			discount_allocation_raw.amount_set_shop_money_amount_sek,
            discount_allocation_raw.amount_set_shop_money_currency_code,
            discount_allocation_raw.discount_application_index,
            discount_allocation_raw.index,
            discount_allocation_raw.order_line_id,
            discount_allocation_raw.shopify_shop,
            global_curr_eur.currency_abbreviation,
            global_curr_eur.conversion_rate_eur
           FROM discount_allocation_raw
             LEFT JOIN global_curr_eur USING (currency_abbreviation, creation_date)
        )
 SELECT *
   FROM discount_allocation_enriched

    -----incrememntal table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."discount_allocation_it")
  