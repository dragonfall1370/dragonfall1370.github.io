
      
    delete from "airup_eu_dwh"."shopify_global"."fact_shopify_product_level_report_unification"
    where (order_id) in (
        select (order_id)
        from "fact_shopify_product_level_report_unificat__dbt_tmp104752721876"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."fact_shopify_product_level_report_unification" ("customer_id", "sales_channel", "order_date", "customer_type", "order_id", "country", "region", "city", "product_category", "product_subcategory_1", "product_subcategory_2", "product_subcategory_3", "product_status", "gross_revenue", "sales_revenue_1_no_shipping", "sales_revenue_2_no_shipping", "ordered_quantity", "net_quantity", "orders", "customers")
    (
        select "customer_id", "sales_channel", "order_date", "customer_type", "order_id", "country", "region", "city", "product_category", "product_subcategory_1", "product_subcategory_2", "product_subcategory_3", "product_status", "gross_revenue", "sales_revenue_1_no_shipping", "sales_revenue_2_no_shipping", "ordered_quantity", "net_quantity", "orders", "customers"
        from "fact_shopify_product_level_report_unificat__dbt_tmp104752721876"
    )
  