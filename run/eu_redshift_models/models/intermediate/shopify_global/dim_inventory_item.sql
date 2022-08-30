
      
    delete from "airup_eu_dwh"."shopify_global"."dim_inventory_item"
    where (id) in (
        select (id)
        from "dim_inventory_item__dbt_tmp104437800732"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."dim_inventory_item" ("_fivetran_synced", "cost", "country_code_of_origin", "created_at", "id", "province_code_of_origin", "requires_shipping", "sku", "tracked", "updated_at", "shopify_shop")
    (
        select "_fivetran_synced", "cost", "country_code_of_origin", "created_at", "id", "province_code_of_origin", "requires_shipping", "sku", "tracked", "updated_at", "shopify_shop"
        from "dim_inventory_item__dbt_tmp104437800732"
    )
  