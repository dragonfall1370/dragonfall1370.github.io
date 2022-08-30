
      
    delete from "airup_eu_dwh"."shopify_global"."order_shipping_tax_line_at"
    where (order_shipping_line_id) in (
        select (order_shipping_line_id)
        from "order_shipping_tax_line_at__dbt_tmp104042382101"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_shipping_tax_line_at" ("_fivetran_synced", "creation_date", "index", "order_shipping_line_id", "price", "price_chf", "price_gbp", "price_sek", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "index", "order_shipping_line_id", "price", "price_chf", "price_gbp", "price_sek", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "order_shipping_tax_line_at__dbt_tmp104042382101"
    )
  