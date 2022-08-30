
      
    delete from "airup_eu_dwh"."shopify_global"."fact_shopify_order_level_report_unification"
    where (order_id) in (
        select (order_id)
        from "fact_shopify_order_level_report_unificatio__dbt_tmp104752711590"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fact_shopify_order_level_report_unification" ("customer_id", "sales_channel", "order_date", "customer_type", "order_id", "country", "region", "city", "gross_revenue", "net_revenue_1", "net_revenue_2", "net_quantity", "ordered_quantity", "gross_shipping_revenue", "net_shipping_revenue", "orders", "customers")
    (
        select "customer_id", "sales_channel", "order_date", "customer_type", "order_id", "country", "region", "city", "gross_revenue", "net_revenue_1", "net_revenue_2", "net_quantity", "ordered_quantity", "gross_shipping_revenue", "net_shipping_revenue", "orders", "customers"
        from "fact_shopify_order_level_report_unificatio__dbt_tmp104752711590"
    )
  