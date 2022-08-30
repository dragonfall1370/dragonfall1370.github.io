
      
    delete from "airup_eu_dwh"."shopify_global"."tender_transaction_uk"
    where (id) in (
        select (id)
        from "tender_transaction_uk__dbt_tmp104241454868"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."tender_transaction_uk" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "currency", "id", "order_id", "payment_details_credit_card_company", "payment_details_credit_card_number", "payment_method", "processed_at", "remote_reference", "test", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "currency", "id", "order_id", "payment_details_credit_card_company", "payment_details_credit_card_number", "payment_method", "processed_at", "remote_reference", "test", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek"
        from "tender_transaction_uk__dbt_tmp104241454868"
    )
  