

  create view "airup_eu_dwh"."shopify_global"."fct_customer_type_by_country__dbt_tmp" as (
    ---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the customer_id, order_date, customer_type and country 
---###################################################################################################################

 


SELECT 
        customer_id,
        date(foe.created_at) AS order_date,
        CASE
            WHEN (created_at - min(created_at) OVER (PARTITION BY customer_id, shopify_shop)) = '00:00:00'::interval 
            THEN 'New Customer'::text
            ELSE 'Returning Customer'::text
        END AS customer_type,
        case when shopify_shop = 'Base' then 'DE'
        else shopify_shop 
        end AS country
  FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
  WHERE financial_status in ('paid', 'partially_refunded') 
  AND customer_id IS NOT null
  ) with no schema binding;
