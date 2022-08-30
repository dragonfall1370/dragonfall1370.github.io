
      
    delete from "airup_eu_dwh"."shopify_global"."fct_order_adjustment"
    where (id) in (
        select (id)
        from "fct_order_adjustment__dbt_tmp104500895673"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fct_order_adjustment" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_usd", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_sek", "tax_amount_usd", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_usd", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_sek", "tax_amount_usd", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "fct_order_adjustment__dbt_tmp104500895673"
    )
  