---Last Modified by: Nham Dao

select shipment_uuid,shopify_order_datetime::date as shopify_order_date, carrier, status, city, country, postal_code from "airup_eu_dwh"."parcelperform"."fct_shipment_model"
where status = 'return'