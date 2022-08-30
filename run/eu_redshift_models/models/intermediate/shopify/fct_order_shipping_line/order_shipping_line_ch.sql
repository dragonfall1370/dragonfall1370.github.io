
      
    delete from "airup_eu_dwh"."shopify_global"."order_shipping_line_ch"
    where (id) in (
        select (id)
        from "order_shipping_line_ch__dbt_tmp104006944635"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_shipping_line_ch" ("_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "discounted_price_sek", "price_sek")
    (
        select "_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "discounted_price_sek", "price_sek"
        from "order_shipping_line_ch__dbt_tmp104006944635"
    )
  