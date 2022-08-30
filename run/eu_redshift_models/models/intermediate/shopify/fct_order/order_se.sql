
      
    delete from "airup_eu_dwh"."shopify_global"."order_se"
    where (id) in (
        select (id)
        from "order_se__dbt_tmp103952146115"
    );
    

    insert into "airup_eu_dwh"."shopify_global"."order_se" ("id", "_fivetran_synced", "creation_date", "app_id", "billing_address_address_1", "billing_address_address_2", "billing_address_city", "billing_address_company", "billing_address_country", "billing_address_country_code", "billing_address_first_name", "billing_address_id", "billing_address_last_name", "billing_address_latitude", "billing_address_longitude", "billing_address_name", "billing_address_phone", "billing_address_province", "billing_address_province_code", "billing_address_zip", "browser_ip", "buyer_accepts_marketing", "cancel_reason", "cancelled_at", "cart_token", "checkout_token", "closed_at", "confirmed", "created_at", "currency", "current_total_duties_set", "shop_cust_id", "customer_id", "customer_locale", "device_id", "email", "financial_status", "fulfillment_status", "landing_site_base_url", "landing_site_ref", "location_id", "order_number", "note", "note_attributes", "number", "original_order_number", "original_total_duties_set", "payment_gateway_names", "presentment_currency", "processed_at", "processing_method", "reference", "shipping_address_address_1", "shipping_address_address_2", "shipping_address_city", "shipping_address_company", "shipping_address_country", "shipping_address_country_code", "shipping_address_first_name", "shipping_address_id", "shipping_address_last_name", "shipping_address_latitude", "shipping_address_longitude", "shipping_address_name", "shipping_address_phone", "shipping_address_province", "shipping_address_province_code", "shipping_address_zip", "source_identifier", "source_name", "source_url", "subtotal_price", "subtotal_price_sek", "subtotal_price_chf", "subtotal_price_gbp", "subtotal_price_set", "taxes_included", "test", "token", "total_discounts", "total_discounts_chf", "total_discounts_gbp", "total_discounts_sek", "total_discounts_set", "total_line_items_price", "total_line_items_price_sek", "total_line_items_price_chf", "total_line_items_price_gbp", "total_line_items_price_set", "total_price", "total_price_sek", "total_price_chf", "total_price_gbp", "total_price_set", "total_price_usd", "total_shipping_price_set", "total_tax", "total_tax_sek", "total_tax_chf", "total_tax_gbp", "total_tax_set", "total_tip_received", "total_tip_received_sek", "total_tip_received_chf", "total_tip_received_gbp", "total_weight", "updated_at", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "referring_site")
    (
        select "id", "_fivetran_synced", "creation_date", "app_id", "billing_address_address_1", "billing_address_address_2", "billing_address_city", "billing_address_company", "billing_address_country", "billing_address_country_code", "billing_address_first_name", "billing_address_id", "billing_address_last_name", "billing_address_latitude", "billing_address_longitude", "billing_address_name", "billing_address_phone", "billing_address_province", "billing_address_province_code", "billing_address_zip", "browser_ip", "buyer_accepts_marketing", "cancel_reason", "cancelled_at", "cart_token", "checkout_token", "closed_at", "confirmed", "created_at", "currency", "current_total_duties_set", "shop_cust_id", "customer_id", "customer_locale", "device_id", "email", "financial_status", "fulfillment_status", "landing_site_base_url", "landing_site_ref", "location_id", "order_number", "note", "note_attributes", "number", "original_order_number", "original_total_duties_set", "payment_gateway_names", "presentment_currency", "processed_at", "processing_method", "reference", "shipping_address_address_1", "shipping_address_address_2", "shipping_address_city", "shipping_address_company", "shipping_address_country", "shipping_address_country_code", "shipping_address_first_name", "shipping_address_id", "shipping_address_last_name", "shipping_address_latitude", "shipping_address_longitude", "shipping_address_name", "shipping_address_phone", "shipping_address_province", "shipping_address_province_code", "shipping_address_zip", "source_identifier", "source_name", "source_url", "subtotal_price", "subtotal_price_sek", "subtotal_price_chf", "subtotal_price_gbp", "subtotal_price_set", "taxes_included", "test", "token", "total_discounts", "total_discounts_chf", "total_discounts_gbp", "total_discounts_sek", "total_discounts_set", "total_line_items_price", "total_line_items_price_sek", "total_line_items_price_chf", "total_line_items_price_gbp", "total_line_items_price_set", "total_price", "total_price_sek", "total_price_chf", "total_price_gbp", "total_price_set", "total_price_usd", "total_shipping_price_set", "total_tax", "total_tax_sek", "total_tax_chf", "total_tax_gbp", "total_tax_set", "total_tip_received", "total_tip_received_sek", "total_tip_received_chf", "total_tip_received_gbp", "total_weight", "updated_at", "user_id", "shopify_shop", "currency_abbreviation", "conversion_rate_eur", "referring_site"
        from "order_se__dbt_tmp103952146115"
    )
  