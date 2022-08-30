
      
    delete from "airup_eu_dwh"."shopify_global"."discount_allocation_at"
    where (order_line_id) in (
        select (order_line_id)
        from "discount_allocation_at__dbt_tmp103536575236"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."discount_allocation_at" ("_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_set_presentment_money_amount", "amount_set_presentment_money_amount_chf", "amount_set_presentment_money_amount_gbp", "amount_set_presentment_money_amount_sek", "amount_set_presentment_money_currency_code", "amount_set_shop_money_amount", "amount_set_shop_money_amount_chf", "amount_set_shop_money_amount_gbp", "amount_set_shop_money_amount_sek", "amount_set_shop_money_currency_code", "discount_application_index", "index", "order_line_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "amount", "amount_chf", "amount_gbp", "amount_sek", "amount_set_presentment_money_amount", "amount_set_presentment_money_amount_chf", "amount_set_presentment_money_amount_gbp", "amount_set_presentment_money_amount_sek", "amount_set_presentment_money_currency_code", "amount_set_shop_money_amount", "amount_set_shop_money_amount_chf", "amount_set_shop_money_amount_gbp", "amount_set_shop_money_amount_sek", "amount_set_shop_money_currency_code", "discount_application_index", "index", "order_line_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "discount_allocation_at__dbt_tmp103536575236"
    )
  