
      
    delete from "airup_eu_dwh"."shopify_global"."dim_product_tag"
    where (hash_id) in (
        select (hash_id)
        from "dim_product_tag__dbt_tmp104446177086"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."dim_product_tag" ("hash_id", "_fivetran_synced", "index", "product_id", "value", "shopify_shop")
    (
        select "hash_id", "_fivetran_synced", "index", "product_id", "value", "shopify_shop"
        from "dim_product_tag__dbt_tmp104446177086"
    )
  