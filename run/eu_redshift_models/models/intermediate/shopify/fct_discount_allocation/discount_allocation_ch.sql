
      
    delete from "airup_eu_dwh"."shopify_global"."discount_allocation_ch"
    where (order_line_id) in (
        select (order_line_id)
        from "discount_allocation_ch__dbt_tmp103536570198"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."discount_allocation_ch" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_set_presentment_money_amount", "amount_set_presentment_money_amount_chf", "amount_set_presentment_money_amount_gbp", "amount_set_presentment_money_currency_code", "amount_set_shop_money_amount", "amount_set_shop_money_amount_chf", "amount_set_shop_money_amount_gbp", "amount_set_shop_money_currency_code", "discount_application_index", "index", "order_line_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek", "amount_set_presentment_money_amount_sek", "amount_set_shop_money_amount_sek")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_set_presentment_money_amount", "amount_set_presentment_money_amount_chf", "amount_set_presentment_money_amount_gbp", "amount_set_presentment_money_currency_code", "amount_set_shop_money_amount", "amount_set_shop_money_amount_chf", "amount_set_shop_money_amount_gbp", "amount_set_shop_money_currency_code", "discount_application_index", "index", "order_line_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "amount_sek", "amount_set_presentment_money_amount_sek", "amount_set_shop_money_amount_sek"
        from "discount_allocation_ch__dbt_tmp103536570198"
    )
  