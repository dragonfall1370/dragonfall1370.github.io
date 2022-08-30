 

with final as (

SELECT 
      _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_de"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_de" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   


   
UNION all

SELECT 
      _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_at"
  where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_at" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   

UNION all

 SELECT 
      _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_fr"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_fr" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   

UNION all

 SELECT 
    _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_nl"
  where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_nl" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   

UNION all

 SELECT 
     _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_ch"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_ch" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   

UNION all

 SELECT 
     _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_uk"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_uk" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   

UNION all

 SELECT 
      _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_it"
  where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_it" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   

UNION all

 SELECT 
      _fivetran_synced,
          creation_date,
          carrier_identifier,
          code,
          delivery_category,
          discounted_price,
          discounted_price_chf,
          discounted_price_gbp,
            discounted_price_sek,
          discounted_price_set,
          id,
          order_id,
          phone,
          price,
          price_chf,
          price_gbp,
            price_sek,
          price_set,
          requested_fulfillment_service_id,
          source,
          title,
          shopify_shop,
          currency_abbreviation,
          conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_shipping_line_se"
  where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_se" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   


)

select *
from final