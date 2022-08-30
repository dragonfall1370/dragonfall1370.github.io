--created by: Etoma Egot
--last modified: By Etoma Egot on 2022-03-05
--Type of change: Fixed issue with incremental model having multiple "where" clauses




with final as (
with shipment_events as (

  select
     shipment_uuid
     ,phase_key
     ,"time"
     ,carrier_name
     ,event_key
     ,"_fivetran_synced" as etl_loaded_at
   
from "airup_eu_dwh"."parcelperform"."shipments_events"
where event_key in 
    (
  -----------------------------parcel delivery only scenarios-------------------------------------------
             'H10'   ----  successfully delivered
            ,'H11'   ----  successfully delivered to neighbour
            ,'H12'   ----  successfully delivered & proof of delivery collected
            ,'H13'   ----  successfully delivered & cash on delivery collected
            ,'H14'   ----  successfully delivered and left at door
            ,'H20'   ----  successfully collected
            ,'H21'   ----  successfully collected at parcel locker
            ,'H22'   ----  successfully collected at parcel shop
            ,'H23'   ----  successfully collected at post office
            ,'H30'   ----  parcel transferred to third party, further updates may not be received
---------------------------------------------------------------------------------------------------
   ) 

      ),

model as (
 
select 
  shipment_uuid 
 ,shopify_order_datetime 
 ,tracking_number 
 ,shopify_order_nr 
 ,carrier
 ,country
 ,postal_code
 ,city

from "airup_eu_dwh"."parcelperform"."fct_shipment_model"
where status = 'delivered'
and source = 'Shopify'

),

make_model as(
  
select 
  model.*
  ,shipment_events.event_key
  ,shipment_events."time" 
  ,shipment_events.carrier_name
  ,shipment_events.etl_loaded_at
  
from model
left join shipment_events using (shipment_uuid)

), 
summary as ---- in case carrier uses another carrier in destination country and we have 2 timestamp of the same event for 2 carriers
           --- then take the first time of the event
 (select shipment_uuid
 ,shopify_order_datetime
 ,tracking_number
 ,shopify_order_nr
 ,carrier
 ,country
 ,event_key
 ,etl_loaded_at
  ,"time"
 ,carrier_name
 ,postal_code
 ,city
 , row_number () over(partition by shipment_uuid
 ,shopify_order_datetime
 ,tracking_number
 ,shopify_order_nr
 ,carrier
 ,country
 ,postal_code
 ,event_key
 ,etl_loaded_at order by "time") as rank_row
 from make_model)
 select 
 shipment_uuid
 ,shopify_order_datetime
 ,tracking_number
 ,shopify_order_nr
 ,carrier
 ,country
,postal_code
,city
 ,event_key
 ,etl_loaded_at
  ,"time"  as delivery_time
 ,carrier_name AS delivery_carrier_name
 from summary
 where rank_row = 1
)

select * from final

  
   where etl_loaded_at >= (select max(etl_loaded_at) from "airup_eu_dwh"."parcelperform"."dim_parcperf_delvd_shipments")
  