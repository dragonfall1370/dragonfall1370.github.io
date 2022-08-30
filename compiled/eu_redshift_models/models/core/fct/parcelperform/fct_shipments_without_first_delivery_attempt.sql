---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains information of orders which were delivered directly to customer without first delivery attempt
---###################################################################################################################
 

WITH shipment_without_delivery_attempted AS (
         SELECT spsm.shipment_uuid,
            spsm.shopify_order_nr,
            spsm.source,
            spsm.shopify_order_datetime AS order_datetime,
            spsm.carrier,
            spsm.country,
            sfda."time" AS first_delivery_time,
            sfda.event AS status
           FROM "airup_eu_dwh"."parcelperform"."fct_shipment_model" spsm
             LEFT JOIN "airup_eu_dwh"."parcelperform"."fct_shipments_first_delivery_attempt" sfda ON spsm.shipment_uuid::text = sfda.shipment_uuid::text
          WHERE sfda.event IS NULL
        ), shipment_successfully_delivered AS (
         SELECT swda.shipment_uuid,
            swda.shopify_order_nr,
            swda.source,
            swda.order_datetime,
            swda.carrier,
            swda.country,
            swda.status,
            se.event,
            se."time" AS delivered_time,
            se.event_key
           FROM shipment_without_delivery_attempted swda
             LEFT JOIN "airup_eu_dwh"."parcelperform"."shipments_events" se ON swda.shipment_uuid::text = se.shipment_uuid::text
          WHERE se.event_key::text in ('H10','H11', 'H12','H13','H14','H20','H21','H22','H23')
        )
 SELECT t1.shipment_uuid,
    t1.shopify_order_nr,
    t1.source,
    t1.order_datetime,
    t1.carrier,
    t1.country,
    t1.status,
    t1.event,
    t1.delivered_time,
    t1.event_key,
    t1.time_rank
   FROM ( SELECT shipment_successfully_delivered.shipment_uuid,
            shipment_successfully_delivered.shopify_order_nr,
            shipment_successfully_delivered.source,
            shipment_successfully_delivered.order_datetime,
            shipment_successfully_delivered.carrier,
            shipment_successfully_delivered.country,
            shipment_successfully_delivered.status,
            shipment_successfully_delivered.event,
            shipment_successfully_delivered.delivered_time,
            shipment_successfully_delivered.event_key,
            row_number() OVER (PARTITION BY shipment_successfully_delivered.shipment_uuid ORDER BY shipment_successfully_delivered.delivered_time, 'event'::text) AS time_rank
           FROM shipment_successfully_delivered) t1
  WHERE t1.time_rank = 1