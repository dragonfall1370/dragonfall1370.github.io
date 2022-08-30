----legacy: amazon.daily_net_revenue_orders_by_country
---Authors: Etoma Egot
---Last Modified by: Tomas Kristof

---###################################################################################################################

        ---compute daily_net_revenue_orders_by_country---

---###################################################################################################################
 

WITH aggregated_data_per_order AS (
         SELECT ofse.country_fullname AS shipping_country,
            ofse.country_grouping,
            date(ofse.purchase_date) AS date,
            ofse.amazon_order_id,
            sum(ofse.quantity_shipped) AS net_volume,
            sum(ofse.item_price) AS order_item_price,
            sum(ofse.item_tax) AS order_item_tax,
            sum(ofse.item_promotion_discount) AS order_item_promotion_discount,
            sum(ofse.ship_promotion_discount) AS order_ship_promotion_discount,
            sum(ofse.shipping_price) AS order_shipping_price,
            sum(ofse.shipping_tax) AS order_shipping_tax
           FROM "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
          WHERE ofse.returned IS FALSE
          GROUP BY ofse.country_fullname, ofse.country_grouping, (date(ofse.purchase_date)), ofse.amazon_order_id
        )
 SELECT 
    'amz'::text AS sales_channel,
    shipping_country,
    country_grouping,
    date,
    sum(order_item_price) + sum(order_item_tax) +
    sum(order_shipping_price) +
    sum(order_shipping_tax) AS gross_revenue,
    sum(order_item_price) +
    sum(order_shipping_price) AS net_revenue_1,
    sum(order_item_price) +
    sum(order_shipping_price) - sum(order_item_promotion_discount) - sum(order_ship_promotion_discount) AS net_revenue_2,
    count(DISTINCT amazon_order_id) AS net_orders,
    count(DISTINCT amazon_order_id) AS gross_orders,
    sum(net_volume) AS sum
   FROM 
        aggregated_data_per_order
  GROUP BY shipping_country, country_grouping, date