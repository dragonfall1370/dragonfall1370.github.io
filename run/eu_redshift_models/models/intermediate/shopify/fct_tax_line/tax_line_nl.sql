
      
    delete from "airup_eu_dwh"."shopify_global"."tax_line_nl"
    where (order_line_id) in (
        select (order_line_id)
        from "tax_line_nl__dbt_tmp104221479458"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."tax_line_nl" ("_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_chf", "price_gbp", "price_set", "rate", "rate_chf", "rate_gbp", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "price_sek", "rate_sek")
    (
        select "_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_chf", "price_gbp", "price_set", "rate", "rate_chf", "rate_gbp", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "price_sek", "rate_sek"
        from "tax_line_nl__dbt_tmp104221479458"
    )
  