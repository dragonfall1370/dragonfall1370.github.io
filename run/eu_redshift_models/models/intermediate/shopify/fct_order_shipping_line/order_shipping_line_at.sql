
      
    delete from "airup_eu_dwh"."shopify_global"."order_shipping_line_at"
    where (id) in (
        select (id)
        from "order_shipping_line_at__dbt_tmp103954037126"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_shipping_line_at" ("_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_sek", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_sek", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_sek", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_sek", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "order_shipping_line_at__dbt_tmp103954037126"
    )
  