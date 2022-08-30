
      
    delete from "airup_eu_dwh"."shopify_global"."order_line_refund_de"
    where (id) in (
        select (id)
        from "order_line_refund_de__dbt_tmp103919614284"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_line_refund_de" ("_fivetran_synced", "creation_date", "id", "location_id", "order_line_id", "quantity", "refund_id", "restock_type", "subtotal", "subtotal_chf", "subtotal_gbp", "subtotal_set", "total_tax", "total_tax_chf", "total_tax_gbp", "total_tax_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "subtotal_sek", "total_tax_sek")
    (
        select "_fivetran_synced", "creation_date", "id", "location_id", "order_line_id", "quantity", "refund_id", "restock_type", "subtotal", "subtotal_chf", "subtotal_gbp", "subtotal_set", "total_tax", "total_tax_chf", "total_tax_gbp", "total_tax_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "subtotal_sek", "total_tax_sek"
        from "order_line_refund_de__dbt_tmp103919614284"
    )
  