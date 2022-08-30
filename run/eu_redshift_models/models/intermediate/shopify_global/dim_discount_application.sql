
      
    delete from "airup_eu_dwh"."shopify_global"."dim_discount_application"
    where (hash_id) in (
        select (hash_id)
        from "dim_discount_application__dbt_tmp104437789651"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."dim_discount_application" ("hash_id", "_fivetran_synced", "creation_date", "target_type", "target_selection", "allocation_method", "value_type", "description", "code", "title", "type", "index", "order_id", "value", "value_sek", "value_chf", "value_gbp", "value_usd", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "hash_id", "_fivetran_synced", "creation_date", "target_type", "target_selection", "allocation_method", "value_type", "description", "code", "title", "type", "index", "order_id", "value", "value_sek", "value_chf", "value_gbp", "value_usd", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "dim_discount_application__dbt_tmp104437789651"
    )
  