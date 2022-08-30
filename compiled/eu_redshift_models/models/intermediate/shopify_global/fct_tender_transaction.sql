 


SELECT 
     _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_de"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'Base')
  
   
UNION all

SELECT 
     _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_at"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'AT')
  
   
UNION all

 SELECT 
       _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_ch"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'CH')
  
   
UNION all

 SELECT 
        _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_fr"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'FR')
  

UNION all

 SELECT 
        _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_it"

   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'IT')
  
   
UNION all

 SELECT 
         _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_nl"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'NL')
  
   
UNION all

 SELECT 
          _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_uk"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'UK')
  

UNION all

 SELECT 
        _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            null as amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_se"
   
            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '1900-01-01' else max("_fivetran_synced") end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'SE')
  

UNION all

 SELECT 
        _fivetran_synced,
            creation_date,
            amount,
            amount_chf,
            amount_gbp,
			   amount_sek,
            amount_usd,
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
   FROM "airup_eu_dwh"."shopify_global"."tender_transaction_us"


            -----incremental table macro---
  
  where _fivetran_synced >= (select case when max(_fivetran_synced) is null then '2022-06-28' else dateadd(hour, -14 ,max("_fivetran_synced")::timestamp) end from "airup_eu_dwh"."shopify_global"."fct_tender_transaction" where 1=1 and shopify_shop = 'US')
  