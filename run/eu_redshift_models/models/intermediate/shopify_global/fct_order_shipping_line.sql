
      
    delete from "airup_eu_dwh"."shopify_global"."fct_order_shipping_line"
    where (id) in (
        select (id)
        from "fct_order_shipping_line__dbt_tmp104544432105"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fct_order_shipping_line" ("_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_sek", "discounted_price_usd", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_sek", "price_usd", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_sek", "discounted_price_usd", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_sek", "price_usd", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "fct_order_shipping_line__dbt_tmp104544432105"
    )
  