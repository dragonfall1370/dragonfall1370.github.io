

with sku_status as (
select
	distinct product_variant.sku,
	product_enriched.status,
	listagg(distinct product_tag.value, ', ') as product_tags
from
	"airup_eu_dwh"."shopify_global"."dim_product_variant" product_variant
left join "airup_eu_dwh"."shopify_global"."dim_product_enriched" product_enriched on
	product_variant.product_id = product_enriched.id
left join "airup_eu_dwh"."shopify_global"."dim_product_tag" product_tag on
	product_variant.product_id = product_tag.product_id
group by
	product_variant.sku,
	product_enriched.status
        ),
product_categorisation as 
(select
	shopify_mapping_table_current_and_old_products.category,
	shopify_mapping_table_current_and_old_products.subcategory_1,
	shopify_mapping_table_current_and_old_products.subcategory_2,
	shopify_mapping_table_current_and_old_products.subcategory_3,
	shopify_mapping_table_current_and_old_products.pod_mother_group_name,
	shopify_mapping_table_current_and_old_products.pod_mother_price_group,
	shopify_mapping_table_current_and_old_products.sku,
	case
		when sku_status.product_tags ~~ '%discontinued%' then 'discontinued'
		when shopify_mapping_table_current_and_old_products.sku in ('a-1000096', '100000001', '100000002') then 'discontinued'
		else 'active'
	end as product_status,
	shopify_mapping_table_current_and_old_products.pods_per_flavour_pouch
	/* Removing the case statement as it would be manually maintained in the product dimension table and that would result in lesser iterations to change this view. */
	-- case
	-- 	when shopify_mapping_table_current_and_old_products.sku in ('100000040','100000002','100000033','100000005','100000055','100000056','100000007','100000041',
	-- 	'100000003','100000008','100000004','100000006','a-1000096', '140000063', '140000064') then 2
	-- 	when shopify_mapping_table_current_and_old_products.subcategory_1 = '3 Pod Pouch'
	-- 	or (shopify_mapping_table_current_and_old_products.sku in ('140000010','140000011','140000009', '140000061', '140000062')) then 3
	-- 	when shopify_mapping_table_current_and_old_products.subcategory_1 = '6 Pod Bundle' then 6
	-- 	when shopify_mapping_table_current_and_old_products.subcategory_1 = 'Double 6 Pod Pouch' then 12
	-- 	when shopify_mapping_table_current_and_old_products.subcategory_1 = 'Special Bundle Winter Large' then 41
	-- 	when shopify_mapping_table_current_and_old_products.subcategory_1 = 'Special Bundle Winter Small'
	-- 	or (shopify_mapping_table_current_and_old_products.sku in ('140000016','140000023','140000021','100000001','140000031','140000028','140000027','140000029',
	-- 	'140000030','140000022','140000024','140000015','140000017','140000014','140000048','140000047','140000050','140000057','140000058','140000060', '110000134')) then 5
	-- 	when shopify_mapping_table_current_and_old_products.sku in ('a-1000153','a-1000154','a-1000155','a-1000156','140000019') then 9
	-- 	when shopify_mapping_table_current_and_old_products.sku in ('a-1000098', '140000013') then 12
	-- 	when shopify_mapping_table_current_and_old_products.sku in ('140000020','140000012','140000042','140000055','140000038','140000040','140000039','140000041',
	-- 	'140000056','140000037','140000049','140000046') then 8
	-- 	when shopify_mapping_table_current_and_old_products.sku in ('140000036','140000033','140000034','140000032','140000035','140000044','140000043','140000051','140000059') then 10
	-- 	else null
	-- end as pods_per_flavour_pouch
from
	"airup_eu_dwh"."shopify_global"."seed_shopify_mapping_table_current_and_old_products" shopify_mapping_table_current_and_old_products
left join sku_status on
	sku_status.sku = shopify_mapping_table_current_and_old_products.sku
	and (sku_status.status in ('active')))
select *, case when lower(subcategory_3) not like '%benefiz edition%' then
regexp_replace(subcategory_3, '\\([^)]*\\)') else subcategory_3 end as subcategory_3_clean
from product_categorisation