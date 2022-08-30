
      
    delete from "airup_eu_dwh"."shopify_global"."fct_tax_line"
    where (order_line_id) in (
        select (order_line_id)
        from "fct_tax_line__dbt_tmp104550596198"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fct_tax_line" ("_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_chf", "price_gbp", "price_sek", "price_usd", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "rate_usd", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_chf", "price_gbp", "price_sek", "price_usd", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "rate_usd", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "fct_tax_line__dbt_tmp104550596198"
    )
  