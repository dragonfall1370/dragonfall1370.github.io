--- creator: Nham Dao
--- last modify: Nham Dao
--- summary: view for logistics lead time data (including information from shopify, odoo, warehouse, warehouse tracking time)



with shopify_order as (
select
	order_enriched.created_at as shopify_create_date,
	case
		when (order_enriched.shipping_address_country in ('Germany', 'Austria'))
		and order_enriched.order_number !~~ '%-%'::text then concat('DE-', order_enriched.order_number)
		else order_enriched.order_number
	end as shopify_order_number,
	order_enriched.original_order_number,
	order_enriched.financial_status,
	order_enriched.cancel_reason,
	order_enriched.shipping_address_country as shopify_country
from
	"airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
where
	date(order_enriched.created_at) >= (current_date - 90)
	and order_enriched.financial_status in ('paid', 'partially_refunded', 'pending')
	and cancelled_at is null), 
stock_picking as (
select
	stock_picking.id as sp_id,
	stock_picking.sale_teams as sp_sale_teams,
	split_part(stock_picking.carrier_tracking_ref::text, ','::text, 1) as sp_carrier_tracking_ref,
	stock_picking.origin as sp_origin,
	case
		when origin like '%Shop__#%'  THEN REPLACE(origin, 'Shop__#', '')
		when origin like '%Shop_#%'  THEN REPLACE(origin, 'Shop_#', '')
		else origin
	end as shopify_order_number,
	stock_picking.name as sp_name,
	stock_picking.create_date as sp_create_date,
	stock_picking.state as sp_state,
	split_part(stock_picking.name::text, '/'::text, 3) as sp_name_ref
from
	"airup_eu_dwh"."odoo"."stock_picking" stock_picking
where
	date(stock_picking.create_date) >= (current_date- 90)
		and stock_picking.state <> 'cancel'
		and (sp_sale_teams like 'D2C%'
			or sp_sale_teams = 'Influencer')
        ), 
        sale_order as (
select
	case
		when name like '%Shop__#%' THEN REPLACE("name", 'Shop__#', '')
		when name like '%Shop_#%' THEN REPLACE("name", 'Shop_#', '')
		else name
	end as so_name,
	sale_order.create_date as so_create_date,
	sale_order.warehouse_id as so_warehouse_id
from
	odoo.sale_order sale_order
where
	date(sale_order.create_date) >= (current_date - 90)
        ),
ihub_item as (
select
	iqi.item_type as iqi_item_type,
	iqi.id as iqi_id,
	iqi.state as iqi_state,
	iqi.create_date as iqi_create_date,
	split_part(iqi.data::text, ';'::text, 16) as iqi_name_ref
from
	"airup_eu_dwh"."odoo"."ihub_queue_item" iqi
where
	iqi.item_type::text = 'geodis_paccon'::text
	and date(iqi.create_date) >= (current_date - 90)
        ),
ihub_event as 
(select * from "airup_eu_dwh"."odoo"."dim_ihub_event" where date(create_date) >= (current_date - 90)),
order_summary as (
select
	sp.sp_id,
	sp.sp_sale_teams,
	sp.sp_name,
	sp.sp_create_date,
	sp.sp_carrier_tracking_ref,
	sp.sp_state,
	so.so_name,
	so.so_create_date,
	so.so_warehouse_id,
	s.shopify_country,
	s.shopify_order_number,
	s.shopify_create_date,
	ii.iqi_item_type,
	ii.iqi_state,
	ii.iqi_create_date,
	ii.iqi_name_ref,
	ii.iqi_id,
	ie.create_date as ie_create_date,
	case
		when sp_create_date is not null
			and iqi_create_date is not null
            then datediff(day,
			sp_create_date::date,
			iqi_create_date::date)
			else 0
		end as date_diff_sp_iqi,
		case
			when shopify_create_date is not null
				and ie_create_date is not null 
            then datediff(day,
				shopify_create_date::date,
				ie_create_date::date )
				else 0
			end as date_diff_s_ie
		from
			stock_picking sp
		full join sale_order so on
			sp.shopify_order_number::text = so.so_name::text
		full join shopify_order s on
			sp.shopify_order_number::text = s.shopify_order_number
		full join ihub_item ii on
			ii.iqi_name_ref = sp.sp_carrier_tracking_ref
		full join ihub_event ie on
			sp.sp_name::text = ie.order_number
        ) , 
        get_row_for_date_diff_iqi as (
select
	*
from
	reports.series_of_number
where
	gen_num between 0 and (
	select
		max(date_diff_sp_iqi)+ 1
	from
		order_summary )) , 
   day_different_without_weekend_iqi as (
select
	pa1.shopify_order_number,
	pa1.sp_create_date,
	pa1.iqi_create_date,
	case
		when (date_part(dow, iqi_create_date::date) = any (array[0::double precision,
		6::double precision]))
			or (date_part(dow, sp_create_date::date) = any (array[0::double precision,
			6::double precision])) then count(*)
			else count(*) - 1
		end as day_diff_odoo_iqi
	from
		(
		select
			os.shopify_order_number,
			os.sp_create_date,
			os.iqi_create_date,
			date_part(dow, (iqi_create_date::date - gen_num)::date) as day_of_week_iqi
		from
			order_summary as os
		join get_row_for_date_diff_iqi as s on
			1 = 1
		where
			os.sp_create_date is not null
			and os.iqi_create_date is not null
			and date_diff_sp_iqi >= gen_num
			and date_diff_sp_iqi>0 ) pa1
	where
		pa1.day_of_week_iqi = any (array[1::double precision,
		2::double precision,
		3::double precision,
		4::double precision,
		5::double precision])
	group by
		pa1.shopify_order_number,
		pa1.sp_create_date,
		pa1.iqi_create_date
        ), 
        get_row_for_date_diff_ie as (
select
	*
from
	reports.series_of_number
where
	gen_num between 0 and (
	select
		max(date_diff_s_ie)+ 1
	from
		order_summary ))           
        ,
day_different_without_weekend_ie as (
select
	pa2.shopify_order_number,
	pa2.shopify_create_date,
	pa2.ie_create_date,
	case
		when (date_part(dow, ie_create_date::date) = any (array[0::double precision,
		6::double precision]))
			or (date_part(dow, shopify_create_date::date) = any (array[0::double precision,
			6::double precision])) then count(*)
			else count(*) - 1
		end as day_diff_shopify_ie
	from
		(
		select
			os.shopify_order_number,
			os.shopify_create_date,
			os.ie_create_date,
			date_part(dow, (ie_create_date::date - gen_num)::date) as day_of_week_ie
		from
			order_summary os
		join get_row_for_date_diff_ie as s on
			1 = 1
		where
			os.shopify_create_date is not null
			and os.ie_create_date is not null
			and date_diff_s_ie >= gen_num
			and date_diff_s_ie>0) pa2
	where
		pa2.day_of_week_ie = any (array[1::double precision,
		2::double precision,
		3::double precision,
		4::double precision,
		5::double precision])
	group by
		pa2.shopify_order_number,
		pa2.shopify_create_date,
		pa2.ie_create_date
        )
 select
	order_summary.sp_id,
	order_summary.sp_sale_teams,
	order_summary.sp_name,
	order_summary.sp_create_date,
	order_summary.sp_carrier_tracking_ref,
	order_summary.sp_state,
	order_summary.so_warehouse_id,
	order_summary.so_name,
	order_summary.shopify_country,
	order_summary.shopify_order_number,
	order_summary.shopify_create_date,
	order_summary.iqi_item_type,
	order_summary.iqi_state,
	order_summary.iqi_create_date,
	order_summary.iqi_name_ref,
	order_summary.iqi_id,
	order_summary.ie_create_date,
	ddwwq.day_diff_odoo_iqi,
	ddwwe.day_diff_shopify_ie
from
	order_summary
left join day_different_without_weekend_iqi ddwwq on
	order_summary.shopify_order_number = ddwwq.shopify_order_number
left join day_different_without_weekend_ie ddwwe on
	order_summary.shopify_order_number = ddwwe.shopify_order_number