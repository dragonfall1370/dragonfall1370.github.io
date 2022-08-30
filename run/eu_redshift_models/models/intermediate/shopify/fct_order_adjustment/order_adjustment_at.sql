
      
    delete from "airup_eu_dwh"."shopify_global"."order_adjustment_at"
    where (id) in (
        select (id)
        from "order_adjustment_at__dbt_tmp103619850327"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_adjustment_at" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_sek", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_sek", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "order_adjustment_at__dbt_tmp103619850327"
    )
  