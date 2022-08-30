-- legacy view; migrated by tomas kristof, feld m



SELECT 'd2c'::text            AS sales_channel,
       foe.country_fullname   AS shipping_country,
       foe.country_grouping,
       date(foe.created_at)   AS date,
       sum(foe.gross_revenue) AS gross_revenue,
       sum(foe.net_revenue_1) AS net_revenue_1,
       sum(foe.net_revenue_2) AS net_revenue_2,
       sum(foe.net_orders)    AS net_orders,
       sum(foe.gross_orders)  AS gross_orders,
       sum(foe.net_volume)    AS net_volume
FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe
GROUP BY foe.country_fullname, foe.country_grouping, (date(foe.created_at))