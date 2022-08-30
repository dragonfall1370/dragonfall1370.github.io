 
select variant.sku, cat.subcategory_3,product.status from "airup_eu_dwh"."shopify_global"."dim_product" product
left join "airup_eu_dwh"."shopify_global"."dim_product_variant" variant 
on product.id = variant.product_id
left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" cat 
on variant.sku = cat.sku
where _fivetran_deleted = 'false'
and product.status = 'archived'
group by 1,2,3