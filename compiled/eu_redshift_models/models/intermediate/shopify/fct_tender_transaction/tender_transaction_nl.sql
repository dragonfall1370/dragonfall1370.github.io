

WITH tender_transaction_raw AS (
         SELECT 
                _fivetran_synced,
                date(processed_at) AS creation_date,
                amount,
                NULL::double precision AS amount_chf,
                NULL::double precision AS amount_gbp,
				NULL::double precision AS amount_sek,						 
                currency,
                id,
                order_id,
                payment_details_credit_card_company,
                payment_details_credit_card_number,
                payment_method,
                processed_at,
                remote_reference,
                test,
                user_id,
                'Base'::text AS shopify_shop,
                'EUR'::text AS currency_abbreviation
            FROM "airup_eu_dwh"."shopify_nl"."tender_transaction"
          
        ), 
        global_curr_eur AS (
          SELECT date(creation_datetime) AS creation_date,
            currency_abbreviation,
            conversion_rate_eur

          FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"  
          WHERE currency_abbreviation::text = 'EUR'::text
        ),
         tender_transaction_enriched AS (
         SELECT _fivetran_synced,
           creation_date,
            (amount /
                CASE
                    WHEN global_curr_eur.conversion_rate_eur IS NOT NULL THEN global_curr_eur.conversion_rate_eur
                    ELSE 1::double precision
                END)::numeric(10,3)::double precision AS amount,
                  amount_chf,
                  amount_gbp,
				          amount_sek,
                  currency,
                  id,
                  order_id,
                  payment_details_credit_card_company,
                  payment_details_credit_card_number,
                  payment_method,
                  processed_at,
                  remote_reference,
                  test,
                  user_id,
                  shopify_shop,
                    global_curr_eur.currency_abbreviation,
                    global_curr_eur.conversion_rate_eur
           FROM tender_transaction_raw
             LEFT JOIN global_curr_eur USING (currency_abbreviation, creation_date)
        )
 SELECT
            _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			      amount_sek,  
            currency,
            id,
            order_id,
            payment_details_credit_card_company,
            payment_details_credit_card_number,
            payment_method,
            processed_at,
            remote_reference,
            test,
            user_id,
            shopify_shop,
            currency_abbreviation,
            conversion_rate_eur
   FROM 
   tender_transaction_enriched
   
          -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."shopify_global"."tender_transaction_nl")
  