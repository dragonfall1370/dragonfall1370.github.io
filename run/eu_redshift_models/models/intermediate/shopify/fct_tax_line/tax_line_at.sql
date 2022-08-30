
      
    delete from "airup_eu_dwh"."shopify_global"."tax_line_at"
    where (order_line_id) in (
        select (order_line_id)
        from "tax_line_at__dbt_tmp104202740277"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."tax_line_at" ("_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_chf", "price_gbp", "price_sek", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "index", "order_line_id", "price", "price_chf", "price_gbp", "price_sek", "price_set", "rate", "rate_chf", "rate_gbp", "rate_sek", "title", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "tax_line_at__dbt_tmp104202740277"
    )
  