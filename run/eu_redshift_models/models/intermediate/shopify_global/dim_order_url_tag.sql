
      
    delete from "airup_eu_dwh"."shopify_global"."dim_order_url_tag"
    where (order_id) in (
        select (order_id)
        from "dim_order_url_tag__dbt_tmp104437756074"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."dim_order_url_tag" ("order_id", "key", "value")
    (
        select "order_id", "key", "value"
        from "dim_order_url_tag__dbt_tmp104437756074"
    )
  