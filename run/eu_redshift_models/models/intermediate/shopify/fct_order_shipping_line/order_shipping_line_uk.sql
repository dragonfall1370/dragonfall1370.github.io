
      
    delete from "airup_eu_dwh"."shopify_global"."order_shipping_line_uk"
    where (id) in (
        select (id)
        from "order_shipping_line_uk__dbt_tmp104036778219"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_shipping_line_uk" ("_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "discounted_price_sek", "price_sek")
    (
        select "_fivetran_synced", "creation_date", "carrier_identifier", "code", "delivery_category", "discounted_price", "discounted_price_chf", "discounted_price_gbp", "discounted_price_set", "id", "order_id", "phone", "price", "price_chf", "price_gbp", "price_set", "requested_fulfillment_service_id", "source", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "discounted_price_sek", "price_sek"
        from "order_shipping_line_uk__dbt_tmp104036778219"
    )
  