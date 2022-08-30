

WITH shopify_orders AS (
SELECT
CASE WHEN shipping_address_country_code in ('DE', 'AT') AND created_at >= '2021-07-27' and shopify_shop = 'Base' 
and order_enriched.order_number !~~ '%-%' THEN CONCAT('DE-',order_number)
when order_number = '1428021' and shop_country = 'DE' and shipping_address_country = 'Italy'
then 'IT-1428021'
ELSE order_number END AS shopify_order_number, created_at, country_fullname
FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
where order_enriched.financial_status in ('paid', 'partially_refunded', 'pending')
and cancelled_at is null
), odoo_orders AS (
SELECT
CASE
WHEN "name" LIKE '%Shop__#%' THEN REPLACE("name", 'Shop__#', '') --- cannot use ltrim since it strim 'Shop#__#SE-1001' to 'E-1001' while we expect 'SE-1001'
WHEN "name" LIKE '%Shop_#%' THEN REPLACE("name", 'Shop_#', '')
ELSE "name"
END AS odoo_order_number
FROM "airup_eu_dwh"."odoo"."sale_order"
) SELECT
shopify_order_number, created_at, country_fullname
FROM shopify_orders
WHERE shopify_order_number NOT IN (
SELECT odoo_order_number FROM odoo_orders)