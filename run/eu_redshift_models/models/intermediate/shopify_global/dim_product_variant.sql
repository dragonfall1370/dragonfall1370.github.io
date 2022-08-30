
      
    delete from "airup_eu_dwh"."shopify_global"."dim_product_variant"
    where (id) in (
        select (id)
        from "dim_product_variant__dbt_tmp104456217424"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."dim_product_variant" ("_fivetran_synced", "creation_date", "barcode", "compare_at_price", "compare_at_price_chf", "compare_at_price_gbp", "compare_at_price_sek", "compare_at_price_usd", "created_at", "fulfillment_service", "grams", "id", "image_id", "inventory_item_id", "inventory_management", "inventory_policy", "inventory_quantity", "old_inventory_quantity", "option_1", "option_2", "option_3", "position", "price", "price_chf", "price_gbp", "price_sek", "price_usd", "product_id", "requires_shipping", "sku", "tax_code", "taxable", "title", "updated_at", "weight", "weight_unit", "shopify_shop", "currency_abbreviation", "conversion_rate_eur")
    (
        select "_fivetran_synced", "creation_date", "barcode", "compare_at_price", "compare_at_price_chf", "compare_at_price_gbp", "compare_at_price_sek", "compare_at_price_usd", "created_at", "fulfillment_service", "grams", "id", "image_id", "inventory_item_id", "inventory_management", "inventory_policy", "inventory_quantity", "old_inventory_quantity", "option_1", "option_2", "option_3", "position", "price", "price_chf", "price_gbp", "price_sek", "price_usd", "product_id", "requires_shipping", "sku", "tax_code", "taxable", "title", "updated_at", "weight", "weight_unit", "shopify_shop", "currency_abbreviation", "conversion_rate_eur"
        from "dim_product_variant__dbt_tmp104456217424"
    )
  