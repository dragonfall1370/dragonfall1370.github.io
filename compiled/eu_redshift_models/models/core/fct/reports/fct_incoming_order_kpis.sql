

SELECT date(oe.created_at) AS order_date,
    oe.country_fullname AS country,
    ol.order_id,
    sum(ol.quantity) AS total_items,
    count(DISTINCT ol.name) AS total_distinct_items
   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe
     LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" ol ON oe.id = ol.order_id
     LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_enriched" product_enriched ON ol.product_id = product_enriched.id
  GROUP BY (date(oe.created_at)), oe.country_fullname, ol.order_id