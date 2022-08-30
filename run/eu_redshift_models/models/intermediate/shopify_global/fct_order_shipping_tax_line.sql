
      
    delete from "airup_eu_dwh"."shopify_global"."fct_order_shipping_tax_line"
    where (order_shipping_line_id) in (
        select (order_shipping_line_id)
        from "fct_order_shipping_tax_line__dbt_tmp104550564059"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fct_order_shipping_tax_line" ("_fivetran_synced", "creation_date", "index", "order_shipping_line_id", "price", "price_usd", "price_sek", "price_chf", "price_gbp", "price_set", "rate", "rate_sek", "rate_chf", "rate_gbp", "rate_usd", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "index", "order_shipping_line_id", "price", "price_usd", "price_sek", "price_chf", "price_gbp", "price_set", "rate", "rate_sek", "rate_chf", "rate_gbp", "rate_usd", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "fct_order_shipping_tax_line__dbt_tmp104550564059"
    )
  