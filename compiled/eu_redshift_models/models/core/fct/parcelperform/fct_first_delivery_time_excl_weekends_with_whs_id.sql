---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains information of the lead time from orders were created until the first time stamp that 
        --- the delivery was performed
---###################################################################################################################
 

with first_delivery_summary as (
  --- get the first attempt delivery timestamp for each shipment. 
  --- For shipments without first attempt delivery, use the first successfully delivery timestamp
select
	spsm.shipment_uuid,
	spsm.shopify_order_nr,
	spsm.source,
	spsm.shopify_order_datetime::date as order_datetime,
	spsm.carrier,
	country,
	sfda."time"::date as first_delivery_time,
	sfda.event as status,
	datediff(day,
	spsm.shopify_order_datetime::date,
	sfda."time"::date) date_diff_order_first_attempt
	FROM "airup_eu_dwh"."parcelperform"."fct_shipment_model" spsm
--from parcelperform.fct_shipment_model spsm
	LEFT JOIN "airup_eu_dwh"."parcelperform"."fct_shipments_first_delivery_attempt" sfda ON spsm.shipment_uuid::text = sfda.shipment_uuid::text
--left join parcelperform.fct_shipments_first_delivery_attempt sfda on spsm.shipment_uuid::text = sfda.shipment_uuid::text
where
	sfda.event is not null
union
select
	shipments_without_first_delivery_attempt.shipment_uuid,
	shipments_without_first_delivery_attempt.shopify_order_nr,
	shipments_without_first_delivery_attempt.source,
	shipments_without_first_delivery_attempt.order_datetime::date,
	shipments_without_first_delivery_attempt.carrier,
	shipments_without_first_delivery_attempt.country,
	shipments_without_first_delivery_attempt.delivered_time::date as first_delivery_time,
	shipments_without_first_delivery_attempt.event as status,
	datediff(day,
	order_datetime::date,
	delivered_time::date) date_diff_order_first_attempt
	FROM "airup_eu_dwh"."parcelperform"."fct_shipments_without_first_delivery_attempt" shipments_without_first_delivery_attempt
--from 	parcelperform.fct_shipments_without_first_delivery_attempt shipments_without_first_delivery_attempt
        ), 
        shipment_arrived_pickup_point as (
  --- get the arrived at pickup point timestamp
select
	*
	from "airup_eu_dwh"."parcelperform"."fct_shipment_arrived_pickup_point"),
--from	parcelperform.fct_shipment_arrived_pickup_point),
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
	group by 1,2
        ),    
        combined_data as 
        (---combine all the cte to get the necessary columns    
select
	first_delivery_summary.*,
	shipment_arrived_pickup_point.time::date as arrived_at_pickup_point,
	datediff(day,
	order_datetime::date,
	shipment_arrived_pickup_point.time::date) date_diff_order_arrived_pickup,
	so.order_name,
	case
		when so.warehouse_id is not null then so.warehouse_id
		else 'Others'
	end as warehouse_id
from	first_delivery_summary
left join shipment_arrived_pickup_point
        on	first_delivery_summary.shipment_uuid = shipment_arrived_pickup_point.shipment_uuid
left join sale_order so 
        on	first_delivery_summary.shopify_order_nr = so.order_name ),
--- calculate the number of days exclude weekends and holiday for first attempt delivery 
        get_row_for_date_diff as (
select
	*
from	reports.series_of_number
where
	gen_num between 0 and (
	select
		max(date_diff_order_first_attempt)+ 1
	from
		combined_data )),        
        transit_time_excl_weekend as (
select
	pa1.shipment_uuid,
	pa1.shopify_order_nr,
	pa1.source,
	pa1.order_datetime,
	pa1.carrier,
	pa1.country,
	pa1.first_delivery_time,
	pa1.status,
	pa1.warehouse_id,
	case
		when (date_part(dow, order_datetime) in (0, 6))
    then count(*)
			else count(*) -1
		end as date_diff_excl_weekend
	from
		(
		select
			fds.shipment_uuid,
			fds.shopify_order_nr,
			fds.source,
			fds.order_datetime,
			fds.carrier,
			fds.country,
			fds.first_delivery_time,
			fds.status,
			fds.warehouse_id,
			date_part(dow, (first_delivery_time::date - gen_num)::date) as day_of_week
		from
			combined_data as fds
		join get_row_for_date_diff as s on
			1 = 1
			left join "airup_eu_dwh"."reports"."fct_warehouse_holidays" holiday
		--left join reports.fct_warehouse_holidays holiday
      on
			(first_delivery_time::date - gen_num)::date = holiday.holidays::date
				and fds.warehouse_id = holiday.warehouse
			where
				date_diff_order_first_attempt >= gen_num
				and date_diff_order_first_attempt>0
				and holidays is null ) pa1
	where
		pa1.day_of_week in (1, 2, 3, 4, 5)
	group by 1,2,3,4,5,6,7,8,9
        ),
  --- calculate the number of days exclude weekends and holiday for arrived at pickup point
        get_row_for_date_diff_pickup as (
select
	*
from
	reports.series_of_number
where
	gen_num between 0 and (
	select
		max(date_diff_order_arrived_pickup)+ 1
	from
		combined_data )),
   time_till_pickup_point_excl_weekend as (
select
	pa1.shipment_uuid,
	pa1.shopify_order_nr,
	pa1.source,
	pa1.order_datetime,
	pa1.carrier,
	pa1.country,
	pa1.arrived_at_pickup_point,
	pa1.status,
	pa1.warehouse_id,
	case
		when (date_part(dow, order_datetime) in (0, 6))
    then count(*)
			else count(*) -1
		end as date_diff_pickup_excl_weekend
	from
		(
		select
			fds.shipment_uuid,
			fds.shopify_order_nr,
			fds.source,
			fds.order_datetime,
			fds.carrier,
			fds.country,
			fds.arrived_at_pickup_point,
			fds.status,
			fds.warehouse_id,
			date_part(dow, (arrived_at_pickup_point::date - gen_num)::date) as day_of_week
		from
			combined_data as fds
		join get_row_for_date_diff_pickup as s on
			1 = 1
			left join "airup_eu_dwh"."reports"."fct_warehouse_holidays" holiday
		--left join reports.fct_warehouse_holidays holiday
      on
			(arrived_at_pickup_point::date - gen_num)::date = holiday.holidays::date
				and fds.warehouse_id = holiday.warehouse
			where
				date_diff_order_arrived_pickup >= gen_num
				and date_diff_order_arrived_pickup>0
				and holidays is null ) pa1
	where
		pa1.day_of_week in (1, 2, 3, 4, 5)
	group by
		1,2,3,4,5,6,7,8,9
		)
 select
	ttew.shipment_uuid,
	ttew.shopify_order_nr,
	ttew.source,
	ttew.order_datetime,
	ttew.carrier,
	case
		when country_mapping.country_fullname is not null then country_fullname
		else 'Others'
	end as country,
	ttew.first_delivery_time,
	ttew.status,
	ttew.date_diff_excl_weekend,
	ttew.warehouse_id,
	pp.arrived_at_pickup_point,
	pp.date_diff_pickup_excl_weekend
from
	transit_time_excl_weekend ttew
left join time_till_pickup_point_excl_weekend pp
   on ttew.shipment_uuid = pp.shipment_uuid
	left join "airup_eu_dwh"."public"."country_system_account_mapping" country_mapping
--left join public.country_system_account_mapping country_mapping
     on	ttew.country = country_mapping.country_fullname