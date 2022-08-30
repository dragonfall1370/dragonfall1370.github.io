---Authors: Nham Dao
---Last Modified by: Nham Dao

 

WITH sale_order_raw AS (
         SELECT CASE
WHEN "name" LIKE '%Shop__#%' THEN LTRIM("name",'Shop__#')
WHEN "name" LIKE '%Shop_#%' THEN LTRIM("name",'Shop_#')
ELSE "name"
END AS order_name,
            sale_order.warehouse_id,
            row_number() OVER (PARTITION BY sale_order.name ORDER BY (
                CASE
                    WHEN sale_order.warehouse_id IS NULL THEN 1
                    ELSE 0
                END)) AS row_num
           FROM "airup_eu_dwh"."odoo"."sale_order"
        ), sale_order AS (
         SELECT sale_order_raw.order_name,
            sale_order_raw.warehouse_id
           FROM sale_order_raw
          WHERE sale_order_raw.row_num = 1
        ), 
        shipment_model as (
    Select source,
    shipment_uuid,
    pp_created_date,
    shopify_order_datetime,
    tracking_number,
    carrier_reference,
    shopify_order_nr,
    carrier,
    status,
    case when country ='United Kingdom of Great Britain and Northern Ireland' then 'United Kingdom' else country end,
    _fivetran_synced
   FROM "airup_eu_dwh"."parcelperform"."fct_shipment_model"
        ),
 summary as (SELECT sm.source, sm.shipment_uuid,
    sm.pp_created_date::date as pp_created_date,
    sm.shopify_order_datetime::date as shopify_order_datetime,
    sm.tracking_number,
    sm.carrier_reference,
    sm.shopify_order_nr,
    sm.carrier,
    sm.status,
    case when country_mapping.country_fullname is not null then country_fullname else 'Others' end as country,
    case when so.warehouse_id is not null then 
    REGEXP_SUBSTR(so.warehouse_id, '[(](.*)[)]', 1, 1, 'e')  else 'Others' end as warehouse_id
   FROM shipment_model sm
     LEFT JOIN sale_order so ON sm.shopify_order_nr = so.order_name
     left join "airup_eu_dwh"."public"."country_system_account_mapping" country_mapping
     on sm.country = country_mapping.country_fullname)
     select shopify_order_datetime, carrier,status,country,warehouse_id, count(shipment_uuid) as number_of_shipment
     from summary
     group by 1,2,3,4,5