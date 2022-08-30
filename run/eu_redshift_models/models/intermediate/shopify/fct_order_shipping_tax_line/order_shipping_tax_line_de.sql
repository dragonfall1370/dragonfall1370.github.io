
      
    delete from "airup_eu_dwh"."shopify_global"."order_shipping_tax_line_de"
    where (order_shipping_line_id) in (
        select (order_shipping_line_id)
        from "order_shipping_tax_line_de__dbt_tmp104055823377"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_shipping_tax_line_de" ("_fivetran_synced", "creation_date", "index", "order_shipping_line_id", "price", "price_chf", "price_gbp", "price_set", "rate", "rate_chf", "rate_gbp", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "price_sek", "rate_sek")
    (
        select "_fivetran_synced", "creation_date", "index", "order_shipping_line_id", "price", "price_chf", "price_gbp", "price_set", "rate", "rate_chf", "rate_gbp", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "price_sek", "rate_sek"
        from "order_shipping_tax_line_de__dbt_tmp104055823377"
    )
  