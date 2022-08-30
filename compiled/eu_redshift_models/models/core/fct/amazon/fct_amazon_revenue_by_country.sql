----legacy: amazon.amazon_revenue_by_country
---Author: Etoma Egot

---###################################################################################################################

                      ---Compute amazon_revenue_by_country--

---###################################################################################################################

 

WITH 
   aggregated_data_per_order AS (
    SELECT ofse.amazon_order_id                        AS order_id,
           ofse.country_fullname                       AS shipping_country,
           ofse.country_grouping,
           ofse.purchase_date::date                    AS date,
           ofse.product_type,
           ofse.product_name,
           ofse.shipment_item_id,
           cpfm.pod_flavour,
           ofse.sku,
           sum(ofse.quantity_shipped)                  AS sales_quantity,
           sum(ofse.item_price)                        AS order_total_price,
           sum(ofse.item_tax)                          AS order_total_tax,
           sum(ofse.item_promotion_discount)           AS order_item_promotion_discount,
           sum(ofse.ship_promotion_discount)           AS order_ship_promotion_discount,
           sum(ofse.shipping_price)                    AS order_shipping_price,
           sum(ofse.shipping_tax)                      AS order_shipping_tax,
           sum(ofse.item_price) + sum(ofse.item_tax) AS gross_revenue,
           sum(ofse.item_price) + sum(ofse.shipping_price) - sum(ofse.item_promotion_discount) -
           sum(ofse.ship_promotion_discount)           AS net_revenue
    FROM 
           "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
             LEFT JOIN "airup_eu_dwh"."amazon"."custom_pod_flavour_mapping" cpfm ON ofse.product_name::text = cpfm.product_name::text
    WHERE ofse.purchase_date::date >= '2020-01-01'::date
    AND ofse.returned IS FALSE -- filtering out returned orders
    GROUP BY ofse.amazon_order_id, ofse.country_fullname, ofse.country_grouping, (ofse.purchase_date::date),
             ofse.product_type, ofse.product_name, ofse.shipment_item_id, cpfm.pod_flavour, ofse.sku
    ORDER BY (ofse.purchase_date::date), ofse.amazon_order_id
)
SELECT 'amazon'::text                                 AS sales_channel,
       aggregated_data_per_order.shipping_country,
       aggregated_data_per_order.country_grouping,
       aggregated_data_per_order.date                AS purchase_date,
       aggregated_data_per_order.order_id,
       aggregated_data_per_order.product_type,
       aggregated_data_per_order.product_name,
       aggregated_data_per_order.pod_flavour,
       aggregated_data_per_order.sku,
       sum(aggregated_data_per_order.sales_quantity) AS sales_quantity,
       sum(aggregated_data_per_order.gross_revenue)  AS gross_revenue,
       sum(aggregated_data_per_order.net_revenue)    AS net_revenue
FROM aggregated_data_per_order
GROUP BY 'amazon'::text, aggregated_data_per_order.shipping_country, aggregated_data_per_order.country_grouping,
         aggregated_data_per_order.date, aggregated_data_per_order.order_id, aggregated_data_per_order.product_type,
         aggregated_data_per_order.product_name, aggregated_data_per_order.pod_flavour, aggregated_data_per_order.sku
ORDER BY aggregated_data_per_order.date DESC