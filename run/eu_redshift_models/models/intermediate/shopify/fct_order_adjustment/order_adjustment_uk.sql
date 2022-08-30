
      
    delete from "airup_eu_dwh"."shopify_global"."order_adjustment_uk"
    where (id) in (
        select (id)
        from "order_adjustment_uk__dbt_tmp103655342875"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_adjustment_uk" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek", "tax_amount_sek")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek", "tax_amount_sek"
        from "order_adjustment_uk__dbt_tmp103655342875"
    )
  