---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains lead time of active orders from orders created in shopify to the latest event
---###################################################################################################################
 

WITH latest_event AS (
         SELECT t1.shipment_uuid,
            t1.event_time,
            t1.phase,
            t1.event,
            t1.time_rank
           FROM ( SELECT se.shipment_uuid,
                    se."time" AS event_time,
                    se.phase,
                    se.event,
                    row_number() OVER (PARTITION BY se.shipment_uuid ORDER BY se."time" DESC) AS time_rank
                   FROM "airup_eu_dwh"."parcelperform"."shipments_events" se) t1
          WHERE t1.time_rank = 1
        ), active_order AS (
         SELECT sm.shipment_uuid,
            sm.shopify_order_datetime,
            sm.shopify_order_nr,
            sm.status,
            sm.country
           FROM "airup_eu_dwh"."parcelperform"."fct_shipment_model" sm
          WHERE sm.status::text = 'active'::text AND sm.shopify_order_datetime IS NOT NULL
        ),sale_order as (
select
	case
		when sale_order.name like '%Shop__#%' then replace(sale_order.name, 'Shop__#', '')
		--- cannot use ltrim since it strim 'Shop#__#SE-1001' to 'E-1001' while we expect 'SE-1001'
		when sale_order.name like '%Shop_#%' then replace(sale_order.name, 'Shop_#', '')
		else sale_order.name
	end as order_name,
	REGEXP_SUBSTR(sale_order.warehouse_id,'[(](.*)[)]',	1,1,'e') as warehouse_id 
	from "airup_eu_dwh"."odoo"."sale_order" sale_order
	--from odoo.sale_order sale_order
	group by 1,2
        ),    
        active_order_lead_time as 
        (SELECT ao.shipment_uuid,
            ao.shopify_order_datetime::date,
            ao.shopify_order_nr,
            ao.status,
            le.event_time::date,
            le.phase,
            le.event,
            ao.country,
            so.warehouse_id,
			datediff(day, shopify_order_datetime::date, event_time::date) date_diff_order_event
           FROM active_order ao
             LEFT JOIN latest_event le ON ao.shipment_uuid::text = le.shipment_uuid::text
             left join sale_order so on ao.shopify_order_nr = so.order_name )
    , get_row_for_date_diff as (
SELECT *
    FROM reports.series_of_number
    WHERE gen_num BETWEEN 0 AND (select max(date_diff_order_event)+1 FROM active_order_lead_time )) 
 SELECT pa1.shipment_uuid,
    pa1.shopify_order_datetime,
    pa1.shopify_order_nr,
    pa1.status,
    pa1.event_time,
    pa1.phase,
    pa1.event,
    pa1.country,
    pa1.warehouse_id,
    case when (date_part(dow, shopify_order_datetime::date) in (0,6))
    then count(*)
    else count(*) -1 end AS time_diff_excl_weekends
   FROM (
   SELECT
     shipment_uuid,
            shopify_order_datetime,
            shopify_order_nr,
            status,
            event_time,
            phase,
            event,
            country,
            warehouse_id,
     date_part(dow, (event_time::date - gen_num)::date) AS day_of_week
    FROM active_order_lead_time AS dt
      JOIN get_row_for_date_diff AS s ON 1 = 1
      left join "airup_eu_dwh"."reports"."fct_warehouse_holidays" holiday
		--left join reports.fct_warehouse_holidays holiday
      on
			(event_time::date - gen_num)::date = holiday.holidays::date
				and dt.warehouse_id = holiday.warehouse
      where date_diff_order_event >= gen_num and date_diff_order_event >0 
      and holidays is null) pa1
      WHERE pa1.day_of_week in (1,2,3,4,5)
  GROUP BY 1,2,3,4,5,6,7,8,9