
      
    delete from "airup_eu_dwh"."shopify_global"."fct_tender_transaction"
    where (id) in (
        select (id)
        from "fct_tender_transaction__dbt_tmp104652920501"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fct_tender_transaction" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_usd", "currency", "id", "order_id", "payment_details_credit_card_company", "payment_details_credit_card_number", "payment_method", "processed_at", "remote_reference", "test", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_usd", "currency", "id", "order_id", "payment_details_credit_card_company", "payment_details_credit_card_number", "payment_method", "processed_at", "remote_reference", "test", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "fct_tender_transaction__dbt_tmp104652920501"
    )
  