--- creator: Nham Dao
--- last modify: Nham Dao

select 
_file,
key_for_imf,
status,
order_reference,
product_category,
vendor,
item_no_,
item_name,
case when q_ty !='' then
cast(case when trim(' ' FROM q_ty) != '-' then CAST ( replace(q_ty,'.','') AS float ) else Null end as float) 
end as q_ty,
container_no_,
mode_of_transport,
forwarder,
pol,
pod,
case when actual_grd like '%/%' then to_date(actual_grd ,'dd/mm/yyyy',false) else null end as actual_grd,
case when booking_submitted like '%/%' then to_date(booking_submitted ,'dd/mm/yyyy',false) else null end as booking_submitted,
case when booking_cmfd like '%/%' then to_date(booking_cmfd ,'dd/mm/yyyy',false) else null end as booking_cmfd,
case when etd like '%/%' then to_date(etd ,'dd/mm/yyyy',false) else null end as etd ,
case when eta like '%/%' then to_date(eta ,'dd/mm/yyyy',false) else null end as eta ,
case when updated_eta like '%/%' then to_date(updated_eta ,'dd/mm/yyyy',false) else null end as updated_eta ,
case when pick_up_date like '%/%' then to_date(pick_up_date ,'dd/mm/yyyy',false) else null end as pick_up_date ,
case when atd like '%/%' then to_date(atd ,'dd/mm/yyyy',false) else null end as atd ,
case when ata like '%/%' then to_date(ata ,'dd/mm/yyyy',false) else null end as ata ,
case when wh_arrival_date like '%/%' then to_date(wh_arrival_date ,'dd/mm/yyyy',false) else null end as wh_arrival_date ,
case when rfs like '%/%' then to_date(rfs ,'dd/mm/yyyy',false) else null end as rfs ,
delivery_wh,
recorded_alert,
post_arrival_status,
"_56_day_leadtime",
case when container_deivery_fc_date like '%/%' then to_date(container_deivery_fc_date ,'dd/mm/yyyy',false) else null end as container_deivery_fc_date ,
container_deivery_fc_week,
container_delivery_status,
"_fivetran_synced",
reason_for_delay_grd_atd_,
carrier,
reason_for_delay_eta_ata_,
shipment_no_,
current_pre_arrival_alert,
remind_for_shipping_space,
receipt_no_,
case when co_2_e_kg_ !='' then
cast(case when trim(' ' FROM co_2_e_kg_) != '-' then CAST ( replace( replace(co_2_e_kg_, '.', ''), ',', '.')as float ) else Null end as float) 
end as co_2_e_kg_
 from "airup_eu_dwh"."logistics_inbound"."inbound_import_manual"
 where _file in ('/data/incoming/Inbound_data/Us-Import Follow-up.csv', '/data/incoming/Inbound_data/EU-Import Follow-up.csv')