

SELECT date_trunc('day'::text, o.created_at)::date AS order_date,
    count(DISTINCT o.order_number) AS orders
   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" o
  GROUP BY (date_trunc('day'::text, o.created_at)::date)