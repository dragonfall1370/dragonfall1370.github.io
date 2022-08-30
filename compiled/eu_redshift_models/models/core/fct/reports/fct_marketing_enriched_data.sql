-- Migrated by YuShih Hsieh



With d2c AS (
         SELECT 'shopify'::text AS sales_channel,
            order_enriched.shipping_address_country_code AS shipping_country_code,
            date(order_enriched.created_at) AS order_date,
            order_enriched.customer_id,
            product_enriched.title AS product_name,
                CASE
                    WHEN product_enriched.product_type::text = ANY (ARRAY['Accessoires'::character varying::text, 'Accessories'::character varying::text]) THEN 'Accessories'::character varying
                    WHEN product_enriched.product_type::text = ANY (ARRAY['Aromapod'::character varying::text, 'Pod'::character varying::text, 'Aromapod-Bundle-Mix'::character varying::text, 'Aromapod-Bundle'::character varying::text]) THEN 'Pods'::character varying
                    WHEN product_enriched.product_type::text = ANY (ARRAY['Starter Kit'::character varying::text, 'Starter-Set'::character varying::text]) THEN 'Starter Set'::character varying
                    ELSE product_enriched.product_type
                END AS product_type,
                CASE
                    WHEN product_enriched.product_type::text ~~* '%pod%'::text THEN product_variant.price
                    ELSE NULL::double precision
                END AS pod_price_group,
            round(sum(order_line.price * order_line.quantity - tax_line.price - COALESCE(discount_allocation.amount, 0::double precision))::numeric, 4) AS net_sales,
            sum(order_line.quantity) AS sold_items,
            order_enriched.order_number
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" order_line ON order_enriched.id = order_line.order_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_tax_line" tax_line ON order_line.id = tax_line.order_line_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_discount_allocation" discount_allocation ON order_line.id = discount_allocation.order_line_id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_enriched" product_enriched ON order_line.product_id = product_enriched.id
             LEFT JOIN "airup_eu_dwh"."shopify_global"."dim_product_variant" product_variant ON order_line.variant_id = product_variant.id
          GROUP BY order_enriched.shipping_address_country_code, (date(order_enriched.created_at)), product_enriched.title, product_enriched.product_type, product_variant.price, order_enriched.customer_id, order_enriched.order_number
        ), retention_d2c AS (
         SELECT DISTINCT a.customer_id,
            a.order_date,
            a.order_number,
            a.retention_status
           FROM ( SELECT date(order_enriched.created_at) AS order_date,
                    order_enriched.order_number,
                    order_enriched.customer_id,
                        CASE
                            WHEN date(order_enriched.created_at) = min(date(order_enriched.created_at)) OVER (PARTITION BY order_enriched.customer_id) THEN 'new'::text
                            ELSE 'existing'::text
                        END AS retention_status
                   FROM  "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched ) a
          ORDER BY a.customer_id
        ), shopify_with_retention AS (
         SELECT d2c.sales_channel,
            d2c.shipping_country_code,
            d2c.order_date,
            d2c.customer_id,
            d2c.product_name,
            d2c.product_type,
            d2c.pod_price_group,
            d2c.net_sales,
            d2c.sold_items,
            d2c.order_number,
            r.retention_status
           FROM d2c
             LEFT JOIN retention_d2c r USING (customer_id, order_date, order_number)
        ), shopify_retention_data_aggregation AS (
         SELECT shopify_with_retention.sales_channel,
            shopify_with_retention.shipping_country_code,
            shopify_with_retention.order_date,
            shopify_with_retention.retention_status,
            shopify_with_retention.product_name,
            shopify_with_retention.product_type,
            shopify_with_retention.pod_price_group,
            sum(shopify_with_retention.net_sales) AS net_sales,
            sum(shopify_with_retention.sold_items) AS sold_items,
            count(DISTINCT shopify_with_retention.order_number) AS orders,
            count(DISTINCT shopify_with_retention.customer_id) AS customers
           FROM shopify_with_retention
          GROUP BY shopify_with_retention.sales_channel, shopify_with_retention.shipping_country_code, shopify_with_retention.order_date, shopify_with_retention.retention_status, shopify_with_retention.product_name, shopify_with_retention.product_type, shopify_with_retention.pod_price_group
        ), amz AS (
         SELECT ofsmu.buyer_email,
            ofsmu.amazon_order_id AS order_number,
            ofsmu.product_name,
            'Starter Set'::text AS product_type,
            NULL::double precision AS pod_price_group,
            ofsmu.ship_country AS shipping_country_code,
            date(ofsmu.purchase_date) AS order_date,
            round((sum(COALESCE(NULLIF(ofsmu.item_price, 'NaN'::double precision), 0::double precision) * COALESCE(ofsmu.quantity_shipped, 0)::double precision) - sum(COALESCE(NULLIF(ofsmu.item_promotion_discount, 'NaN'::double precision), 0::double precision) * COALESCE(ofsmu.quantity_shipped, 0)::double precision))::numeric, 4) AS net_sales,
            sum(COALESCE(ofsmu.quantity_shipped, 0)) AS sold_items
           FROM "airup_eu_dwh"."amazon"."fulfilled_shipments" ofsmu
          WHERE date(ofsmu.purchase_date) >= date_trunc('month'::text, CURRENT_DATE - '1 mon'::interval)
          GROUP BY ofsmu.buyer_email, ofsmu.amazon_order_id, ofsmu.product_name, ofsmu.ship_country, (date(ofsmu.purchase_date))
        ), retention_amz AS (
         SELECT DISTINCT a.buyer_email,
            a.order_date,
            a.order_number,
            a.retention_status
           FROM ( SELECT date(ofsmu.purchase_date) AS order_date,
                    ofsmu.amazon_order_id AS order_number,
                    ofsmu.buyer_email,
                        CASE
                            WHEN date(ofsmu.purchase_date) = min(date(ofsmu.purchase_date)) OVER (PARTITION BY ofsmu.buyer_email) THEN 'new'::text
                            ELSE 'existing'::text
                        END AS retention_status
                   FROM "airup_eu_dwh"."amazon"."fulfilled_shipments" ofsmu) a 
            ORDER BY a.buyer_email 
        ), amazon_with_retention AS (
         SELECT amz.buyer_email,
            amz.order_number,
            amz.product_name,
            amz.product_type,
            amz.pod_price_group,
            amz.shipping_country_code,
            amz.order_date,
            amz.net_sales,
            amz.sold_items,
            r1.retention_status
           FROM amz
             LEFT JOIN retention_amz r1 USING (buyer_email, order_date, order_number)
        ), amazon_retention_data_aggregation AS (
         SELECT 'amazon'::text AS sales_channel,
            amazon_with_retention.shipping_country_code,
            amazon_with_retention.order_date,
            amazon_with_retention.retention_status,
            amazon_with_retention.product_name,
            amazon_with_retention.product_type,
            amazon_with_retention.pod_price_group,
            sum(amazon_with_retention.net_sales) AS net_sales,
            sum(amazon_with_retention.sold_items) AS sold_items,
            count(DISTINCT amazon_with_retention.order_number) AS orders,
            count(DISTINCT amazon_with_retention.buyer_email) AS customers
           FROM amazon_with_retention
          GROUP BY 'amazon'::text, amazon_with_retention.shipping_country_code, amazon_with_retention.order_date, amazon_with_retention.retention_status, amazon_with_retention.product_name, amazon_with_retention.product_type, amazon_with_retention.pod_price_group
        ), combined_data AS (
         SELECT shopify_retention_data_aggregation.sales_channel,
            shopify_retention_data_aggregation.shipping_country_code,
            shopify_retention_data_aggregation.order_date,
            shopify_retention_data_aggregation.retention_status,
            shopify_retention_data_aggregation.product_name,
            shopify_retention_data_aggregation.product_type,
            shopify_retention_data_aggregation.pod_price_group,
            shopify_retention_data_aggregation.net_sales,
            shopify_retention_data_aggregation.sold_items,
            shopify_retention_data_aggregation.orders,
            shopify_retention_data_aggregation.customers
           FROM shopify_retention_data_aggregation
        UNION ALL
         SELECT amazon_retention_data_aggregation.sales_channel,
            amazon_retention_data_aggregation.shipping_country_code,
            amazon_retention_data_aggregation.order_date,
            amazon_retention_data_aggregation.retention_status,
            amazon_retention_data_aggregation.product_name,
            amazon_retention_data_aggregation.product_type,
            amazon_retention_data_aggregation.pod_price_group,
            amazon_retention_data_aggregation.net_sales,
            amazon_retention_data_aggregation.sold_items,
            amazon_retention_data_aggregation.orders,
            amazon_retention_data_aggregation.customers
           FROM amazon_retention_data_aggregation
        )
 SELECT combined_data.sales_channel,
    combined_data.shipping_country_code,
    combined_data.order_date,
    combined_data.retention_status,
    combined_data.product_name,
    combined_data.product_type,
    combined_data.pod_price_group,
    combined_data.net_sales,
    combined_data.sold_items,
    combined_data.orders,
    combined_data.customers,
    csam.shopify_shipping_address_country AS shipping_country
   FROM combined_data
     LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" csam ON combined_data.shipping_country_code = ltrim(rtrim(csam.country_abbreviation, '}'), '{')::text