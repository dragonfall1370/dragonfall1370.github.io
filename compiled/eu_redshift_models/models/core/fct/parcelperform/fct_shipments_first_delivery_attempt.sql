---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains information of orders with first delivery attempt
---###################################################################################################################
 

WITH original_carrier AS (
         SELECT sm.shipment_uuid,
            se."time",
            se.event,
            sm.carrier
           FROM "airup_eu_dwh"."parcelperform"."shipments_events" se
            LEFT JOIN "airup_eu_dwh"."parcelperform"."fct_shipment_model" sm ON se.shipment_uuid::text = sm.shipment_uuid::text
        ), delivery_attempted AS (
         SELECT original_carrier.shipment_uuid,
            original_carrier."time",
            original_carrier.event,
            original_carrier.carrier
           FROM original_carrier
          WHERE original_carrier.event::text ~~ 'Delivery attempted%'::text
        )
 SELECT t1.shipment_uuid,
    t1."time",
    t1.event,
    t1.carrier,
    t1.attempt_rank
   FROM ( SELECT delivery_attempted.shipment_uuid,
            delivery_attempted."time",
            delivery_attempted.event,
            delivery_attempted.carrier,
            row_number() OVER (PARTITION BY delivery_attempted.shipment_uuid ORDER BY delivery_attempted."time") AS attempt_rank
           FROM delivery_attempted) t1
  WHERE t1.attempt_rank = 1