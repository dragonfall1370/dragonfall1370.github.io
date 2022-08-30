
      
    delete from "airup_eu_dwh"."shopify_global"."order_adjustment_ch"
    where (id) in (
        select (id)
        from "order_adjustment_ch__dbt_tmp103622136989"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_adjustment_ch" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek", "tax_amount_sek")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_set", "id", "kind", "order_id", "reason", "refund_id", "tax_amount", "tax_amount_chf", "tax_amount_gbp", "tax_amount_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek", "tax_amount_sek"
        from "order_adjustment_ch__dbt_tmp103622136989"
    )
  