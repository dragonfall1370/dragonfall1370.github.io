
      
    delete from "airup_eu_dwh"."shopify_global"."dim_product"
    where (id) in (
        select (id)
        from "dim_product__dbt_tmp104437805723"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."dim_product" ("_fivetran_deleted", "_fivetran_synced", "created_at", "handle", "id", "product_type", "published_at", "published_scope", "status", "title", "updated_at", "vendor", "shopify_shop")
    (
        select "_fivetran_deleted", "_fivetran_synced", "created_at", "handle", "id", "product_type", "published_at", "published_scope", "status", "title", "updated_at", "vendor", "shopify_shop"
        from "dim_product__dbt_tmp104437805723"
    )
  