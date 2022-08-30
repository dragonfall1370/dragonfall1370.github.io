

WITH d2c_revenue AS (
         SELECT pa1.cy_order_date::date AS order_date,
            'D2C'::text AS purchase_channel,
            pa1.country,
            pa1.region,
            COALESCE(pa1.cy_net_revenue_2, 0) AS cy_net_revenue_2,
            COALESCE(pa2.py_net_revenue_2, 0) AS py_net_revenue_2,
            COALESCE(pa3.pm_net_revenue_2, 0) AS pm_net_revenue_2
           FROM ( SELECT order_enriched.created_at AS cy_order_date,
                    order_enriched.country_fullname AS country,
                    order_enriched.country_grouping AS region,
                    sum(order_enriched.net_revenue_2) AS cy_net_revenue_2
                   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
                  WHERE order_enriched.financial_status IN ('paid', 'partially_refunded') AND order_enriched.customer_id IS NOT NULL
                  GROUP BY (order_enriched.created_at), order_enriched.country_fullname, order_enriched.country_grouping) pa1
             FULL JOIN ( SELECT order_enriched.created_at AS py_order_date,
                    order_enriched.country_fullname AS country,
                    order_enriched.country_grouping AS region,
                    sum(order_enriched.net_revenue_2) AS py_net_revenue_2
                   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
                  WHERE order_enriched.financial_status IN ('paid', 'partially_refunded') AND order_enriched.customer_id IS NOT NULL
                  GROUP BY (order_enriched.created_at), order_enriched.country_fullname, order_enriched.country_grouping) pa2 ON (pa1.cy_order_date::date -
                CASE
                    WHEN (date_part('year', pa1.cy_order_date::date) % 4) = 0 THEN '366 days'::interval
                    ELSE '365 days'::interval
                END) = pa2.py_order_date AND pa1.country = pa2.country AND pa1.region = pa2.region
             FULL JOIN ( SELECT order_enriched.created_at AS pm_order_date,
                    order_enriched.country_fullname AS country,
                    order_enriched.country_grouping AS region,
                    sum(order_enriched.net_revenue_2) AS pm_net_revenue_2
                   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
                  WHERE order_enriched.financial_status IN ('paid', 'partially_refunded') AND order_enriched.customer_id IS NOT NULL
                  GROUP BY (order_enriched.created_at), order_enriched.country_fullname, order_enriched.country_grouping) pa3 ON (pa1.cy_order_date::date -
                CASE
                    WHEN date_part('month', pa1.cy_order_date::date) = 1 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 2 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) <= 28 THEN '28 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) > 28 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 4 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 6 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 8 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 9 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 11 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    ELSE NULL::interval
                END) = pa3.pm_order_date AND pa1.country = pa3.country AND pa1.region = pa3.region
          WHERE pa1.cy_order_date::date IS NOT NULL
          ORDER BY pa1.country, pa1.cy_order_date::date
        ), amz_revenue AS (
         SELECT pa1.cy_order_date::date AS order_date,
            'Amazon'::text AS purchase_channel,
            pa1.country,
            pa1.region,
            COALESCE(pa1.cy_net_revenue_2, 0) AS cy_net_revenue_2,
            COALESCE(pa2.py_net_revenue_2, 0) AS py_net_revenue_2,
            COALESCE(pa3.pm_net_revenue_2, 0) AS pm_net_revenue_2
           FROM ( SELECT ofsmue.country_fullname AS country,
                    ofsmue.purchase_date AS cy_order_date,
                    sum(COALESCE(ofsmue.item_price, 0)) + sum(COALESCE(ofsmue.shipping_price, 0)) + sum(COALESCE(ofsmue.gift_wrap_price, 0)) + sum(COALESCE(ofsmue.item_promotion_discount, 0)) + sum(COALESCE(ofsmue.ship_promotion_discount, 0)) AS cy_net_revenue_2,
                    ofsmue.country_grouping AS region
                   FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofsmue
                  WHERE ofsmue.returned IS FALSE
                  GROUP BY ofsmue.country_fullname, (ofsmue.purchase_date), ofsmue.country_grouping) pa1
             FULL JOIN ( SELECT ofsmue.country_fullname AS country,
                    ofsmue.purchase_date AS py_order_date,
                    sum(COALESCE(ofsmue.item_price, 0)) + sum(COALESCE(ofsmue.shipping_price, 0)) + sum(COALESCE(ofsmue.gift_wrap_price, 0)) + sum(COALESCE(ofsmue.item_promotion_discount, 0)) + sum(COALESCE(ofsmue.ship_promotion_discount, 0)) AS py_net_revenue_2,
                    ofsmue.country_grouping AS region
                   FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofsmue
                  WHERE ofsmue.returned IS FALSE
                  GROUP BY ofsmue.country_fullname, (ofsmue.purchase_date), ofsmue.country_grouping) pa2 ON (pa1.cy_order_date::date -
                CASE
                    WHEN (date_part('year', pa1.cy_order_date::date) % 4) = 0 THEN '366 days'::interval
                    ELSE '365 days'::interval
                END) = pa2.py_order_date AND pa1.country = pa2.country AND pa1.region = pa2.region
             FULL JOIN ( SELECT ofsmue.country_fullname AS country,
                    ofsmue.purchase_date AS pm_order_date,
                    sum(COALESCE(ofsmue.item_price, 0)) + sum(COALESCE(ofsmue.shipping_price, 0)) + sum(COALESCE(ofsmue.gift_wrap_price, 0)) + sum(COALESCE(ofsmue.item_promotion_discount, 0)) + sum(COALESCE(ofsmue.ship_promotion_discount, 0)) AS pm_net_revenue_2,
                    ofsmue.country_grouping AS region
                  FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofsmue
                  WHERE ofsmue.returned IS FALSE
                  GROUP BY ofsmue.country_fullname, (ofsmue.purchase_date), ofsmue.country_grouping) pa3 ON (pa1.cy_order_date::date -
                CASE
                    WHEN date_part('month', pa1.cy_order_date::date) = 1 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 2 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) <= 28 THEN '28 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) > 28 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 4 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 6 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 8 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 9 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 11 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    ELSE NULL::interval
                END) = pa3.pm_order_date AND pa1.country = pa3.country AND pa1.region = pa3.region
          WHERE pa1.cy_order_date::date IS NOT NULL
          ORDER BY pa1.country, pa1.cy_order_date::date
        ), bol_revenue AS (
         SELECT pa1.cy_order_date::date AS order_date,
            'BOL'::text AS purchase_channel,
            pa1.country,
            pa1.region,
            COALESCE(pa1.cy_net_revenue_2, 0) AS cy_net_revenue_2,
            COALESCE(pa2.py_net_revenue_2, 0) AS py_net_revenue_2,
            COALESCE(pa3.pm_net_revenue_2, 0) AS pm_net_revenue_2
           FROM ( SELECT order_enriched.created_at AS cy_order_date,
                    order_enriched.country_fullname AS country,
                    order_enriched.country_grouping AS region,
                    sum(order_enriched.net_revenue_2) AS cy_net_revenue_2
                  FROM "airup_eu_dwh"."shopify_marketplace"."fct_order_enriched_marketplace" order_enriched
                  WHERE order_enriched.financial_status IN ('paid', 'partially_refunded') AND order_enriched.customer_id IS NOT NULL
                  GROUP BY (order_enriched.created_at), order_enriched.country_fullname, order_enriched.country_grouping) pa1
             FULL JOIN ( SELECT order_enriched.created_at AS py_order_date,
                    order_enriched.country_fullname AS country,
                    order_enriched.country_grouping AS region,
                    sum(order_enriched.net_revenue_2) AS py_net_revenue_2
                   FROM "airup_eu_dwh"."shopify_marketplace"."fct_order_enriched_marketplace" order_enriched
                   WHERE order_enriched.financial_status IN ('paid', 'partially_refunded') AND order_enriched.customer_id IS NOT NULL
                  GROUP BY (order_enriched.created_at), order_enriched.country_fullname, order_enriched.country_grouping) pa2 ON (pa1.cy_order_date::date -
                CASE
                    WHEN (date_part('year', pa1.cy_order_date::date) % 4) = 0 THEN '366 days'::interval
                    ELSE '365 days'::interval
                END) = pa2.py_order_date AND pa1.country = pa2.country AND pa1.region = pa2.region
             FULL JOIN ( SELECT order_enriched.created_at AS pm_order_date,
                    order_enriched.country_fullname AS country,
                    order_enriched.country_grouping AS region,
                    sum(order_enriched.net_revenue_2) AS pm_net_revenue_2
                   FROM "airup_eu_dwh"."shopify_marketplace"."fct_order_enriched_marketplace" order_enriched
                  WHERE order_enriched.financial_status IN ('paid', 'partially_refunded') AND order_enriched.customer_id IS NOT NULL
                  GROUP BY (order_enriched.created_at), order_enriched.country_fullname, order_enriched.country_grouping) pa3 ON (pa1.cy_order_date::date -
                CASE
                    WHEN date_part('month', pa1.cy_order_date::date) = 1 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 2 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) <= 28 THEN '28 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) > 28 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 4 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 6 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 8 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 9 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 11 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    ELSE NULL::interval
                END) = pa3.pm_order_date AND pa1.country = pa3.country AND pa1.region = pa3.region
          WHERE pa1.cy_order_date::date IS NOT NULL
          ORDER BY pa1.country, pa1.cy_order_date::date
        ), offline_revenue_excel AS (
         SELECT pa1.cy_order_date::date AS order_date,
            'Offline Sales Excel'::text AS purchase_channel,
            pa1.country,
            pa1.region,
            COALESCE(pa1.cy_net_revenue_2, 0) AS cy_net_revenue_2,
            COALESCE(pa2.py_net_revenue_2, 0) AS py_net_revenue_2,
            COALESCE(pa3.pm_net_revenue_2, 0) AS pm_net_revenue_2
           FROM ( SELECT offline_sales_excel.country,
                    offline_sales_excel.date AS cy_order_date,
                    offline_sales_excel.net_revenue AS cy_net_revenue_2,
                    offline_sales_excel.region
                   FROM "airup_eu_dwh"."odoo"."fct_moneyballs_offline_sales_excel" offline_sales_excel) pa1
             FULL JOIN ( SELECT offline_sales_excel.country,
                    offline_sales_excel.date AS py_order_date,
                    offline_sales_excel.net_revenue AS py_net_revenue_2,
                    offline_sales_excel.region
                    FROM "airup_eu_dwh"."odoo"."fct_moneyballs_offline_sales_excel" offline_sales_excel) pa2 ON (pa1.cy_order_date::date -
                CASE
                    WHEN (date_part('year', pa1.cy_order_date::date) % 4) = 0 THEN '366 days'::interval
                    ELSE '365 days'::interval
                END) = pa2.py_order_date AND pa1.country = pa2.country AND pa1.region = pa2.region
             FULL JOIN ( SELECT offline_sales_excel.country,
                    offline_sales_excel.date AS pm_order_date,
                    offline_sales_excel.net_revenue AS pm_net_revenue_2,
                    offline_sales_excel.region
                    FROM "airup_eu_dwh"."odoo"."fct_moneyballs_offline_sales_excel" offline_sales_excel) pa3 ON (pa1.cy_order_date::date -
                CASE
                    WHEN date_part('month', pa1.cy_order_date::date) = 1 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 2 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) <= 28 THEN '28 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 3 AND date_part('day', pa1.cy_order_date::date) > 28 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 4 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 5 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 6 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 7 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 8 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 9 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 10 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 11 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '31 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) <= 30 THEN '30 days'::interval
                    WHEN date_part('month', pa1.cy_order_date::date) = 12 AND date_part('day', pa1.cy_order_date::date) > 30 THEN '00:00:00'::interval
                    ELSE NULL::interval
                END) = pa3.pm_order_date AND pa1.country = pa3.country AND pa1.region = pa3.region
          WHERE pa1.cy_order_date::date IS NOT NULL
          ORDER BY pa1.country, pa1.cy_order_date::date
        )
 SELECT d2c_revenue.country,
    d2c_revenue.order_date,
    d2c_revenue.purchase_channel,
    d2c_revenue.cy_net_revenue_2,
    d2c_revenue.py_net_revenue_2,
    d2c_revenue.pm_net_revenue_2,
    d2c_revenue.region
   FROM d2c_revenue
UNION ALL
 SELECT amz_revenue.country,
    amz_revenue.order_date,
    amz_revenue.purchase_channel,
    amz_revenue.cy_net_revenue_2,
    amz_revenue.py_net_revenue_2,
    amz_revenue.pm_net_revenue_2,
    amz_revenue.region
   FROM amz_revenue
UNION ALL
 SELECT bol_revenue.country,
    bol_revenue.order_date,
    bol_revenue.purchase_channel,
    bol_revenue.cy_net_revenue_2,
    bol_revenue.py_net_revenue_2,
    bol_revenue.pm_net_revenue_2,
    bol_revenue.region
   FROM bol_revenue
UNION ALL
 SELECT offline_revenue_excel.country,
    offline_revenue_excel.order_date,
    offline_revenue_excel.purchase_channel,
    offline_revenue_excel.cy_net_revenue_2,
    offline_revenue_excel.py_net_revenue_2,
    offline_revenue_excel.pm_net_revenue_2,
    offline_revenue_excel.region
   FROM offline_revenue_excel