

SELECT oe.updated_at::date AS last_updated,
    oe.created_at::date AS date,
    oe.country_fullname AS country,
    ol.sku,
    oe.gross_revenue AS sales,
    COALESCE(oe.net_volume, 0::double precision) AS items_sold,
    oe.gross_orders AS orders,
    COALESCE(oe.net_revenue_1, 0::double precision) AS net_sales,
    'Bol.com'::text AS channel,
    csm.asin,
    csm.airup_sku,
    spc.category AS product_type,
    spc.subcategory_3 AS product_name
   FROM "airup_eu_dwh"."shopify_marketplace"."fct_order_enriched_marketplace" oe
     LEFT JOIN "airup_eu_dwh"."shopify_marketplace"."fct_order_line_marketplace" ol ON oe.id = ol.order_id
     LEFT JOIN "airup_eu_dwh"."amazon"."custom_sku_mapping" csm ON ol.sku::text = csm.sku::text
     LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" spc ON csm.airup_sku = spc.sku
  WHERE oe.country_fullname IS NOT NULL