---Authors: Etoma Egot
---Last Modified by: Nham Dao


with delivery_time as 
( SELECT shipment_uuid,
                    shopify_order_datetime::date,
                    carrier,
                    delivery_carrier_name,
                    shopify_order_nr,
                    'delivered'::text AS status,
                    case when country = 'United Kingdom of Great Britain and Northern Ireland' then 'United Kingdom'
        			      when country = 'United States of America' then 'United States' else country end as country ,
                    postal_code,
                    city,
                    delivery_time::date,
					datediff(day, shopify_order_datetime::date, delivery_time::date) date_diff_order_deliver
                   --FROM parcelperform.dim_parcperf_delvd_shipments
                  from "airup_eu_dwh"."parcelperform"."dim_parcperf_delvd_shipments"
                   ),
  sale_order as (
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
  delivery_time_with_whs_id as (
    select shipment_uuid,
                    shopify_order_datetime,
                    carrier,
                    delivery_carrier_name,
                    shopify_order_nr,
                    status,
                    country,
                    postal_code,
                    city,
                    delivery_time,
					          date_diff_order_deliver,
                    sale_order.warehouse_id
    from delivery_time
    left join sale_order
    on delivery_time.shopify_order_nr = sale_order.order_name),
 get_row_for_date_diff as (
SELECT *
    FROM reports.series_of_number
    WHERE gen_num BETWEEN 0 AND (select max(date_diff_order_deliver)+1 FROM delivery_time_with_whs_id )) 
  ,day_different_without_weekend AS (
         SELECT pa1.shipment_uuid,
    pa1.shopify_order_datetime as order_datetime,
    pa1.carrier,
    pa1.delivery_carrier_name,
    pa1.country,
    pa1.postal_code,
    pa1.city,
    pa1.delivery_time,
    pa1.warehouse_id,    
    pa1.status,
    case when date_part(dow, shopify_order_datetime::date) in (0,6)
    then count(*)
    else count(*) -1 end as delivery_time_excl_weekends
           FROM (SELECT
     dt.shipment_uuid,
    dt.shopify_order_datetime,
    dt.carrier,
    dt.delivery_carrier_name,
    dt.country,
    dt.postal_code,
    dt.city,
    dt.delivery_time,
    dt.status,
    dt.warehouse_id,
     date_part(dow, (delivery_time::date - gen_num)::date) AS day_of_week
    FROM delivery_time_with_whs_id AS dt
      JOIN get_row_for_date_diff AS s ON 1 = 1
      left join "airup_eu_dwh"."reports"."fct_warehouse_holidays" holiday
		--left join reports.fct_warehouse_holidays holiday
      on
			(delivery_time::date - gen_num)::date = holiday.holidays::date
				and dt.warehouse_id = holiday.warehouse
      where date_diff_order_deliver >= gen_num and date_diff_order_deliver>0
      and holidays is null ) pa1
          WHERE pa1.day_of_week in (1,2,3,4,5)
          GROUP by 1,2,3,4,5,6,7,8,9,10
        ) select * from day_different_without_weekend