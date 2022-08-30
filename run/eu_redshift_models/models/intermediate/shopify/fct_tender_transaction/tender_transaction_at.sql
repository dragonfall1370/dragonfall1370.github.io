
      
    delete from "airup_eu_dwh"."shopify_global"."tender_transaction_at"
    where (id) in (
        select (id)
        from "tender_transaction_at__dbt_tmp104225394124"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."tender_transaction_at" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "currency", "id", "order_id", "payment_details_credit_card_company", "payment_details_credit_card_number", "payment_method", "processed_at", "remote_reference", "test", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "currency", "id", "order_id", "payment_details_credit_card_company", "payment_details_credit_card_number", "payment_method", "processed_at", "remote_reference", "test", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "tender_transaction_at__dbt_tmp104225394124"
    )
  