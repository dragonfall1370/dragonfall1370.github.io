

   WITH sales_past_week AS (
            SELECT ol.sku,
               ol.shopify_shop,
               sum(ol.quantity) AS sales_past_week
            FROM "airup_eu_dwh"."shopify_global"."fct_order_line" ol
            WHERE ol.creation_date >= (CURRENT_DATE - 7) AND ol.creation_date <= CURRENT_DATE
            GROUP BY ol.sku, ol.shopify_shop
         ), inventory_quantity AS (
            SELECT 
               iv.sku,
               iv.inventory_quantity,
               iv.shopify_shop,
               ROW_NUMBER() OVER (PARTITION BY sku, shopify_shop ORDER BY full_date DESC) rn
            FROM "airup_eu_dwh"."reports"."fct_inventory_levels" iv
         ), inventory_quantity_latest AS (
            SELECT 
               sku,
               inventory_quantity,
               shopify_shop 
            FROM inventory_quantity
            WHERE inventory_quantity.rn = 1
         )
   SELECT cspm.sku,
      cspm.subcategory_3,
      cspm.subcategory_2,
      COALESCE(iq.inventory_quantity, 0::bigint) AS inventory_quantity,
      spw.shopify_shop,
      COALESCE(spw.sales_past_week, 0::double precision) AS sales_past_week
      FROM inventory_quantity_latest iq 
      LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" cspm ON cspm.sku::text = iq.sku::text
      LEFT JOIN sales_past_week spw ON iq.sku::text = spw.sku::text AND iq.shopify_shop = spw.shopify_shop
      WHERE cspm.product_status = 'active'