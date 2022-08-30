-----Author: Etoma Egot
-----Last Modified By: Etoma Egot

---###################################################################################################################

        ---This query fetches exchange rate data from Odoo res_currency and res_currency rate tables
                
    ---Note: This model is heavily required for all shopify webshop models to maintain single currency rates in euro
---###################################################################################################################

---incrementing the model and specifiying the target destination of the table in the DWH--- 
   
    
 
 WITH currency AS (

     SELECT id AS currency_id,
                symbol,
                name AS currency_abbreviation,
                currency_unit_label AS currency_name

           FROM "airup_eu_dwh"."odoo_currency"."res_currency"
          
        ), 
        
        rates AS (
     SELECT currency_id,
                create_date AS creation_datetime,
                name AS creation_date,
                rate AS conversion_rate_eur,
                _fivetran_synced

     FROM "airup_eu_dwh"."odoo_currency"."res_currency_rate"
      

        ),
     final as (
      SELECT 
    
         rates.currency_id,
         rates.creation_datetime,
         rates.creation_date,
         currency.symbol,
         currency.currency_abbreviation,
         currency.currency_name,
         rates.conversion_rate_eur,
         rates._fivetran_synced

     FROM rates
         LEFT JOIN currency USING (currency_id)
     )

     SELECT 
        md5(cast(coalesce(cast(creation_datetime as varchar), '') || '-' || coalesce(cast(currency_id as varchar), '') as varchar)) as hash_id,
         currency_id,
         creation_datetime,
         creation_date,
         symbol,
         currency_abbreviation,
         currency_name,
         conversion_rate_eur,
         _fivetran_synced
     FROM final
     
          -----incremental table macro---
    
   where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates")
  