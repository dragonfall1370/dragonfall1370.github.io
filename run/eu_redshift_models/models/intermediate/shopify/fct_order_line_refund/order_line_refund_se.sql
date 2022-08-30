
      
    delete from "airup_eu_dwh"."shopify_global"."order_line_refund_se"
    where (id) in (
        select (id)
        from "order_line_refund_se__dbt_tmp103927991829"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_line_refund_se" ("_fivetran_synced", "creation_date", "id", "location_id", "order_line_id", "quantity", "refund_id", "restock_type", "subtotal", "subtotal_chf", "subtotal_gbp", "subtotal_sek", "subtotal_set", "total_tax", "total_tax_chf", "total_tax_gbp", "total_tax_sek", "total_tax_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "id", "location_id", "order_line_id", "quantity", "refund_id", "restock_type", "subtotal", "subtotal_chf", "subtotal_gbp", "subtotal_sek", "subtotal_set", "total_tax", "total_tax_chf", "total_tax_gbp", "total_tax_sek", "total_tax_set", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "order_line_refund_se__dbt_tmp103927991829"
    )
  