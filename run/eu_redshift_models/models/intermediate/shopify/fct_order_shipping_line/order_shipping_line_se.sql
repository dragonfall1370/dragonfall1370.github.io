
      
    delete from "airup_eu_dwh"."shopify_global"."order_shipping_line_se"
    where (id) in (
        select (id)
        from "order_shipping_line_se__dbt_tmp104034547067"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_shipping_line_se" ("_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_sek", "discounted_price_chf", "discounted_price_gbp", "discounted_price_set", "id", "order_id", "phone", "price", "price_sek", "price_chf", "price_gbp", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_sek", "discounted_price_chf", "discounted_price_gbp", "discounted_price_set", "id", "order_id", "phone", "price", "price_sek", "price_chf", "price_gbp", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "order_shipping_line_se__dbt_tmp104034547067"
    )
  