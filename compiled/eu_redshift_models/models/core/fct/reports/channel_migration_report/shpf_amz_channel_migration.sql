--legacy: migrated by tomas k.




WITH amazon_buyer_details_per_order AS (
    SELECT concat(fulfilled_shipments_tax.buyer_name, '_', fulfilled_shipments_tax.bill_postal_code) AS customer_id,
           fulfilled_shipments_tax.amazon_order_id                                                   AS oid,
           fulfilled_shipments_tax.purchase_date
    FROM "airup_eu_dwh"."amazon"."fulfilled_shipments_tax" fulfilled_shipments_tax
    GROUP BY (concat(fulfilled_shipments_tax.buyer_name, '_', fulfilled_shipments_tax.bill_postal_code)),
             fulfilled_shipments_tax.amazon_order_id, fulfilled_shipments_tax.purchase_date
),
     amazon_orders AS (
         SELECT amazon_buyer_details_per_order.customer_id,
                aggregated_data_per_order.amazon_order_id AS oid,
                amazon_buyer_details_per_order.purchase_date,
                aggregated_data_per_order.net_revenue_2,
                'amazon'::text                            AS sales_channel
         FROM  "airup_eu_dwh"."amazon"."fct_aggregated_data_per_order"  aggregated_data_per_order
                  LEFT JOIN amazon_buyer_details_per_order
                            ON amazon_buyer_details_per_order.oid::text = aggregated_data_per_order.amazon_order_id::text
     ),
     shopify_orders AS (
         SELECT concat(order_enriched.billing_address_name, '_', order_enriched.billing_address_zip) AS customer_id,
                order_enriched.id::character varying                                                 AS oid,
                order_enriched.created_at                                                            AS purchase_date,
                order_enriched.net_revenue_2,
                'shopify'::text                                                                      AS sales_channel
         FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
         WHERE order_enriched.financial_status::text = ANY
               (ARRAY ['paid'::character varying::text, 'partially_refunded'::character varying::text])
     ),
     all_orders AS (
         SELECT amazon_orders.customer_id,
                amazon_orders.oid,
                amazon_orders.purchase_date,
                amazon_orders.net_revenue_2,
                amazon_orders.sales_channel
         FROM amazon_orders
         UNION
         SELECT shopify_orders.customer_id,
                shopify_orders.oid,
                shopify_orders.purchase_date,
                shopify_orders.net_revenue_2,
                shopify_orders.sales_channel
         FROM shopify_orders
     ),
     all_orders_2 AS (
         SELECT all_orders.customer_id,
                all_orders.oid,
                all_orders.purchase_date,
                all_orders.net_revenue_2,
                all_orders.sales_channel,
                min(all_orders.purchase_date) OVER (PARTITION BY all_orders.customer_id)                               AS min_order_date_per_customer,
                min(all_orders.purchase_date)
                OVER (PARTITION BY all_orders.customer_id, all_orders.sales_channel)                                   AS min_order_date_per_customer_and_channel,
                max(all_orders.purchase_date)
                OVER (PARTITION BY all_orders.customer_id)                                                             AS max_order_date_per_customer,
                max(all_orders.purchase_date)
                OVER (PARTITION BY all_orders.customer_id, all_orders.sales_channel)                                   AS max_order_date_per_customer_and_channel,
                sum(1)
                OVER (PARTITION BY all_orders.customer_id, all_orders.sales_channel ORDER BY all_orders.purchase_date) AS nth_order
         FROM all_orders
     ),
     final_table AS (
         SELECT all_orders_2.customer_id,
                all_orders_2.oid,
                all_orders_2.purchase_date,
                all_orders_2.sales_channel,
                all_orders_2.min_order_date_per_customer,
                all_orders_2.min_order_date_per_customer_and_channel,
                all_orders_2.max_order_date_per_customer,
                all_orders_2.max_order_date_per_customer_and_channel,
                all_orders_2.nth_order,
                max(
                CASE
                    WHEN all_orders_2.min_order_date_per_customer = all_orders_2.purchase_date
                        THEN all_orders_2.sales_channel
                    ELSE NULL::text
                    END) OVER (PARTITION BY all_orders_2.customer_id) AS first_order_sales_channel,
                max(
                CASE
                    WHEN all_orders_2.max_order_date_per_customer = all_orders_2.purchase_date
                        THEN all_orders_2.sales_channel
                    ELSE NULL::text
                    END) OVER (PARTITION BY all_orders_2.customer_id) AS last_order_sales_channel,
                all_orders_2.net_revenue_2
         FROM all_orders_2
     )
SELECT final_table.customer_id,
       final_table.oid,
       final_table.purchase_date,
       final_table.sales_channel,
       final_table.min_order_date_per_customer,
       final_table.min_order_date_per_customer_and_channel,
       final_table.max_order_date_per_customer,
       final_table.max_order_date_per_customer_and_channel,
       final_table.nth_order,
       final_table.first_order_sales_channel,
       final_table.last_order_sales_channel,
       final_table.net_revenue_2
FROM final_table