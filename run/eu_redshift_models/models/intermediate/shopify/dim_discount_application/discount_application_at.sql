
      
    delete from "airup_eu_dwh"."shopify_global"."discount_application_at"
    where (hash_id) in (
        select (hash_id)
        from "discount_application_at__dbt_tmp103557321337"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."discount_application_at" ("hash_id", "_fivetran_synced", "creation_date", "target_type", "target_selection", "allocation_method", "value_type", "description", "code", "title", "type", "index", "order_id", "value", "value_chf", "value_gbp", "value_sek", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "hash_id", "_fivetran_synced", "creation_date", "target_type", "target_selection", "allocation_method", "value_type", "description", "code", "title", "type", "index", "order_id", "value", "value_chf", "value_gbp", "value_sek", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "discount_application_at__dbt_tmp103557321337"
    )
  