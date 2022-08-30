
      
    delete from "airup_eu_dwh"."shopify_global"."product_variant_at"
    where (id) in (
        select (id)
        from "product_variant_at__dbt_tmp104123129804"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."product_variant_at" ("_fivetran_synced", "creation_date", "barcode", "compare_at_price", "compare_at_price_chf", "compare_at_price_gbp", "compare_at_price_sek", "created_at", "fulfillment_service", "grams", "id", "image_id", "inventory_item_id", "inventory_management", "inventory_policy", "inventory_quantity", "old_inventory_quantity", "option_1", "option_2", "option_3", "position", "price", "price_chf", "price_gbp", "price_sek", "product_id", "requires_shipping", "sku", "tax_code", "taxable", "title", "updated_at", "weight", "weight_unit", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "barcode", "compare_at_price", "compare_at_price_chf", "compare_at_price_gbp", "compare_at_price_sek", "created_at", "fulfillment_service", "grams", "id", "image_id", "inventory_item_id", "inventory_management", "inventory_policy", "inventory_quantity", "old_inventory_quantity", "option_1", "option_2", "option_3", "position", "price", "price_chf", "price_gbp", "price_sek", "product_id", "requires_shipping", "sku", "tax_code", "taxable", "title", "updated_at", "weight", "weight_unit", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "product_variant_at__dbt_tmp104123129804"
    )
  