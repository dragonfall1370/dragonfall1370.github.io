

SELECT amazon_sales.last_updated,
    amazon_sales.date,
    amazon_sales.country,
    amazon_sales.product_type,
    amazon_sales.product_name,
    amazon_sales.sku,
    amazon_sales.items_sold,
    amazon_sales.orders,
    amazon_sales.sales,
    amazon_sales.net_sales,
    amazon_sales.channel,
    amazon_sales.asin,
    amazon_sales.airup_sku
   FROM "airup_eu_dwh"."amazon"."fct_amazon_sales" amazon_sales
UNION ALL
 SELECT bs.last_updated,
    bs.date,
    bs.country,
    bs.product_type,
    bs.product_name,
    bs.sku,
    bs.sales AS items_sold,
    bs.items_sold AS orders,
    bs.orders AS sales,
    bs.net_sales,
    bs.channel,
    bs.asin,
    bs.airup_sku
   FROM "airup_eu_dwh"."shopify_marketplace"."fct_bol_sales" bs