
      
    delete from "airup_eu_dwh"."shopify_global"."tax_line_se"
    where (order_line_id) in (
        select (order_line_id)
        from "tax_line_se__dbt_tmp104225362296"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."tax_line_se" ("_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_sek", "price_chf", "price_gbp", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_sek", "price_chf", "price_gbp", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "tax_line_se__dbt_tmp104225362296"
    )
  