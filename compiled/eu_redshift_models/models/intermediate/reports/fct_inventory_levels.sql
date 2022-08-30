

WITH ssd AS (
SELECT DISTINCT 
	sku,
	shopify_shop,
	full_date 
FROM "airup_eu_dwh"."shopify_global"."dim_product_variant" dpv
CROSS JOIN
"airup_eu_dwh"."reports"."dates" d
WHERE d.full_date >= '2021-11-01' AND d.full_date <= CURRENT_DATE
), joined AS (
SELECT DISTINCT
	ssd.sku,
	ssd.shopify_shop,
	ssd.full_date,
	dpv.inventory_quantity 
FROM ssd 
LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_variant" dpv
ON ssd.sku = dpv.sku AND ssd.shopify_shop = dpv.shopify_shop AND ssd.full_date = dpv."_fivetran_synced"::date
WHERE dpv.sku <> ''
--WHERE ssd.sku = '100000006'
), cte AS (
SELECT
    full_date,
    shopify_shop,
    sku,
    inventory_quantity,
    SUM(CASE WHEN inventory_quantity IS NULL THEN 0 ELSE 1 END) OVER (PARTITION BY sku, shopify_shop  ORDER BY full_date ROWS UNBOUNDED PRECEDING) as value_partition
  FROM joined
)
SELECT 
full_date,
shopify_shop,
sku,
--inventory_quantity,
--value_partition,
FIRST_VALUE (inventory_quantity) OVER (PARTITION BY value_partition, sku, shopify_shop  ORDER BY full_date ROWS UNBOUNDED PRECEDING) AS inventory_quantity
FROM cte
ORDER BY shopify_shop, full_date