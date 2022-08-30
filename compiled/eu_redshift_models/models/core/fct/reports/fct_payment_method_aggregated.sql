---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the e-commerce product category and subcategory 
---###################################################################################################################


 

WITH payment_method_agg AS (
         SELECT date(oe.created_at) AS date,
            oe.country_fullname AS country,
            tt.payment_method
           FROM "airup_eu_dwh"."shopify_global"."fct_tender_transaction" tt
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe ON tt.order_id = oe.id
        )
 SELECT payment_method_agg.date,
    payment_method_agg.country,
    payment_method_agg.payment_method
   FROM payment_method_agg