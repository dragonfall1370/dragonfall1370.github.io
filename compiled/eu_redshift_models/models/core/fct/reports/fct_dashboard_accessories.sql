

SELECT order_enriched.created_at::date AS order_date,
    order_enriched.country_fullname,
    shopify_product_categorisation.category,
    shopify_product_categorisation.subcategory_1,
    shopify_product_categorisation.subcategory_2,
    shopify_product_categorisation.subcategory_3_clean as subcategory_3,
    sum(order_line.quantity) AS products_sold
   FROM "airup_eu_dwh"."shopify_global"."fct_order_line" order_line
     LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched ON order_enriched.id = order_line.order_id
     LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_variant" product_variant ON order_line.sku::text = product_variant.sku::text
     LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation ON shopify_product_categorisation.sku::text =
        CASE
            WHEN product_variant.barcode::text = '4260633610991'::text THEN '150000011'::character varying
            ELSE product_variant.sku
        END::text
  WHERE shopify_product_categorisation.product_status = 'active'::text AND  order_enriched.financial_status::text IN ('paid', 'partially_refunded')
  GROUP BY order_enriched.created_at::date, order_enriched.country_fullname, shopify_product_categorisation.category, shopify_product_categorisation.subcategory_1, shopify_product_categorisation.subcategory_2, shopify_product_categorisation.subcategory_3