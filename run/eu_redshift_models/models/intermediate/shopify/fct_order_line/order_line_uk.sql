
      
    delete from "airup_eu_dwh"."shopify_global"."order_line_uk"
    where (id) in (
        select (id)
        from "order_line_uk__dbt_tmp103952152878"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_line_uk" ("_fivetran_synced", "creation_date", "destination_location_address_1", "destination_location_address_2", "destination_location_city", "destination_location_country_code", "destination_location_id", "destination_location_name", "destination_location_province_code", "destination_location_zip", "fulfillable_quantity", "fulfillment_service", "fulfillment_status", "gift_card", "grams", "id", "index", "name", "order_id", "origin_location_address_1", "origin_location_address_2", "origin_location_city", "origin_location_country_code", "origin_location_id", "origin_location_name", "origin_location_province_code", "origin_location_zip", "pre_tax_price", "pre_tax_price_chf", "pre_tax_price_gbp", "pre_tax_price_set", "price", "price_chf", "price_gbp", "price_set", "product_exists", "product_id", "properties", "quantity", "requires_shipping", "sku", "tax_code", "taxable", "title", "total_discount", "total_discount_chf", "total_discount_gbp", "total_discount_set", "variant_id", "variant_inventory_management", "variant_title", "vendor", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "pre_tax_price_sek", "price_sek", "total_discount_sek")
    (
        select "_fivetran_synced", "creation_date", "destination_location_address_1", "destination_location_address_2", "destination_location_city", "destination_location_country_code", "destination_location_id", "destination_location_name", "destination_location_province_code", "destination_location_zip", "fulfillable_quantity", "fulfillment_service", "fulfillment_status", "gift_card", "grams", "id", "index", "name", "order_id", "origin_location_address_1", "origin_location_address_2", "origin_location_city", "origin_location_country_code", "origin_location_id", "origin_location_name", "origin_location_province_code", "origin_location_zip", "pre_tax_price", "pre_tax_price_chf", "pre_tax_price_gbp", "pre_tax_price_set", "price", "price_chf", "price_gbp", "price_set", "product_exists", "product_id", "properties", "quantity", "requires_shipping", "sku", "tax_code", "taxable", "title", "total_discount", "total_discount_chf", "total_discount_gbp", "total_discount_set", "variant_id", "variant_inventory_management", "variant_title", "vendor", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "pre_tax_price_sek", "price_sek", "total_discount_sek"
        from "order_line_uk__dbt_tmp103952152878"
    )
  