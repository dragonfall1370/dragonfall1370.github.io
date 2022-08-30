


WITH return_pass AS (
	SELECT 
		date_trunc('day',folrwca.report_date)::date AS return_date,
		foe.shopify_shop,
		folwca.sku,
		spc.category,
		spc.subcategory_1,
		spc.subcategory_2,
		spc.subcategory_3,
		SUM(folrwca.quantity) AS return_quantity,
		SUM(folrwca.subtotal) AS return_price
	FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe 
	LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line_w_created_at" folwca ON foe.id = folwca.order_id
	LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line_refund_w_created_at" folrwca ON folwca.id = folrwca.order_line_id 
	LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" spc ON spc.sku = folwca.sku
	WHERE folrwca.id IS NOT NULL
	GROUP BY 1, 2, 3, 4, 5, 6, 7
), order_pass AS (
	SELECT 
		date_trunc('day',foe.created_at)::date AS order_date,
		foe.shopify_shop,
		folwca.sku,
		spc.category,
		spc.subcategory_1,
		spc.subcategory_2,
		spc.subcategory_3,
		SUM(folwca.quantity) AS order_quantity,
		SUM(folwca.price) AS order_price
	FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" foe 
	LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line_w_created_at" folwca ON foe.id = folwca.order_id
	LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" spc ON spc.sku = folwca.sku
	GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT 
	COALESCE(order_pass.order_date, return_pass.return_date) AS "date",
	COALESCE(order_pass.shopify_shop, return_pass.shopify_shop) AS shopify_shop,
	COALESCE(order_pass.sku, return_pass.sku) AS sku,
	COALESCE(order_pass.category, return_pass.category) AS category,
	COALESCE(order_pass.subcategory_1, return_pass.subcategory_1) AS subcategory_1,
	COALESCE(order_pass.subcategory_2, return_pass.subcategory_2) AS subcategory_2,
	COALESCE(order_pass.subcategory_3, return_pass.subcategory_3) AS subcategory_3,
	COALESCE(SUM(order_pass.order_quantity), 0) AS order_quantity,
	COALESCE(SUM(order_pass.order_price), 0) AS order_price,
	COALESCE(SUM(return_pass.return_quantity), 0) AS return_quantity,
	COALESCE(SUM(return_pass.return_price), 0) AS return_price
FROM return_pass
FULL OUTER JOIN order_pass ON return_pass.return_date = order_pass.order_date
AND return_pass.shopify_shop = order_pass.shopify_shop
AND return_pass.sku = order_pass.sku
AND return_pass.category = order_pass.category
AND return_pass.subcategory_1 = order_pass.subcategory_1
AND return_pass.subcategory_2 = order_pass.subcategory_2
AND return_pass.subcategory_3 = order_pass.subcategory_3
GROUP BY 1, 2, 3, 4, 5, 6, 7