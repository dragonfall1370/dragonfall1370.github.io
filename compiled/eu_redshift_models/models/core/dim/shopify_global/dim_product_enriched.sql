

   SELECT product_type_mapping.product_type_mapped,
    dp._fivetran_deleted,
    dp._fivetran_synced,
    dp.created_at,
    dp.handle,
    dp.id,
    dp.product_type,
    dp.published_at,
    dp.published_scope,
    dp.status,
    dp.title,
    dp.updated_at,
    dp.vendor,
    dp.shopify_shop
   FROM "airup_eu_dwh"."shopify_global"."dim_product" dp
     LEFT JOIN "airup_eu_dwh"."shopify_global"."product_type_mapping" ON dp.product_type::text = product_type_mapping.product_type_raw::text