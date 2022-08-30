----legacy: amazon.aggregated_data_per_order
---Authors: Erik Klemusch,  Etoma Egot
---Last Modified by: Tomas Kristof

---###################################################################################################################

        ---compute aggregated amazon fulfilled shipments per order---

---###################################################################################################################
 
SELECT 
       ofse.country_fullname                 AS shipping_country,
       ofse.country_grouping,
       date(ofse.purchase_date)              AS date,
       ofse.amazon_order_id,
       md5(ofse.buyer_email::text)           AS buyer_email_hashed,
       sum(ofse.item_price)                  AS order_item_price,
       sum(ofse.item_tax)                    AS order_item_tax,
       sum(ofse.item_promotion_discount)     AS order_item_promotion_discount,
       sum(ofse.ship_promotion_discount)     AS order_ship_promotion_discount,
       sum(ofse.shipping_price)              AS order_shipping_price,
       sum(ofse.shipping_tax)                AS order_shipping_tax,
       sum(ofse.item_price) +
       sum(ofse.item_tax) +
       sum(ofse.shipping_tax)                AS gross_revenue,
       sum(ofse.item_price) +
       sum(ofse.shipping_price)              AS net_revenue_1,
       sum(ofse.item_price) +
       sum(ofse.shipping_price) +
       sum(ofse.item_promotion_discount) +
       sum(ofse.ship_promotion_discount)     AS net_revenue_2,
       count(DISTINCT ofse.amazon_order_id)  AS net_orders,
       count(DISTINCT ofse.amazon_order_id)  AS gross_orders,
       sum(ofse.quantity_shipped)            AS net_volume
FROM 
      "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched" ofse
WHERE ofse.returned IS FALSE -- filtering out returned orders
GROUP BY ofse.country_fullname,
         ofse.country_grouping,
         (date(ofse.purchase_date)),
         ofse.amazon_order_id,
         (md5(ofse.buyer_email::text))