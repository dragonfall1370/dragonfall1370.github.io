

WITH missing_orders AS (
    SELECT 
        COUNT(shopify_order_number) AS missing_orders, 
        date_trunc('day', created_at)::date AS created_at, country_fullname
    FROM "airup_eu_dwh"."odoo"."fct_missing_orders_granular"
    GROUP BY date_trunc('day', created_at)::date, country_fullname
), all_orders AS (
    SELECT 
        COUNT(id) AS all_orders, 
        date_trunc('day', created_at)::date AS created_at, country_fullname
    FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched"  order_enriched
    where order_enriched.financial_status in ('paid', 'partially_refunded')  
    and cancelled_at is null
    GROUP BY date_trunc('day', created_at)::date, country_fullname
) SELECT 
    all_orders.created_at,
    all_orders.all_orders,
    COALESCE( missing_orders.missing_orders, 0) AS missing_orders, all_orders.country_fullname
FROM all_orders
LEFT JOIN missing_orders ON all_orders.created_at = missing_orders.created_at and all_orders.country_fullname = missing_orders.country_fullname