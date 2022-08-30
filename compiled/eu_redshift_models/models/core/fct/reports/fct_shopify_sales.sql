

WITH cte AS (
         SELECT o.created_at::date AS date,
            ol.product_id,
            ol.name AS product_name,
            pv.sku,
            ol.order_id,
            o.country_fullname AS country,
            sum(ol.price * ol.quantity) AS line_item_sales,
            sum(ol.price * ol.quantity - COALESCE(tl.price, 0::double precision) - COALESCE(da.amount, 0::double precision)) AS line_item_net_sales,
            sum(ol.quantity) AS items_sold,
            max(o._fivetran_synced)::date AS orders_refresh_date
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" o
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" ol ON o.id = ol.order_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_tax_line" tl ON ol.id = tl.order_line_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_discount_allocation" da ON ol.id = da.order_line_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_variant" pv ON ol.variant_id = pv.id
          GROUP BY (o.created_at::date), ol.product_id, ol.name, pv.sku, ol.order_id, o.country_fullname
        )
 SELECT cte.orders_refresh_date AS last_updated,
    cte.date,
    cte.country,
    p.category AS product_type,
    cte.product_name,
    cte.sku,
    sum(cte.items_sold) AS items_sold,
    count(DISTINCT cte.order_id) AS orders,
    round(sum(cte.line_item_sales::numeric), 2) AS sales,
    round(sum(cte.line_item_net_sales::numeric), 2) AS net_sales
   FROM cte
     LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" p ON cte.sku = p.sku
  WHERE cte.country IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6
  ORDER BY cte.date DESC