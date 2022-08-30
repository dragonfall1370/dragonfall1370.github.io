---Authors: Etoma Egot
---Last Modified by: Nham Dao


with transf_cte as (
select "source"    
        ,shipment_uuid
       ,created_date as pp_created_date
       ,updated_date as pp_updated_date
      ,case when json_extract_path_text(additional_info,'Order Created Date',true) <> ''
      then TO_TIMESTAMP(json_extract_path_text(additional_info,'Order Created Date',true),'YYYY-MM-DD HH24:MI:SS') 
      else Null end as shopify_order_datetime  
       ,tracking_number 
       ,carrier_reference 
      ,replace(json_extract_path_text("order" ,'order_reference',true)::text, '#','') as shopify_order_nr
       ,json_extract_path_text(carrier, 'name',true)::text as carrier
       ,status 
       ,json_extract_path_text(weight,'amount',true)::text as weight_grams
       ,json_extract_path_text(current_phase, 'key',true)::text as "phase_key" 
       ,json_extract_path_text(current_phase,'name',true)::text as "current_phase" 
       ,tracking_page_reference as webstore
       ,item_count as quantity
       ,regexp_replace(trim(notification_email::text),'\\[|"|\\]', '') as email 
       ,json_extract_path_text(shipment_value,'amount',true) as amount 
       ,json_extract_path_text(shipment_value ,'currency',true)::text as "currency" 
       ,json_extract_path_text(to_address, 'country',true)::text as "country" 
       ,json_extract_path_text(to_address,'city',true)::text as "city" 
       ,json_extract_path_text(to_address ,'full',true)::text "address" 
       ,json_extract_path_text(to_address ,'postal_code',true)::text "postal_code" 
       ,json_extract_path_text(recipient_address, 'first_name',true)::text as "first_name" 
       ,json_extract_path_text(recipient_address, 'last_name',true)::text as "last_name" 
       ,payment_type
       ,_fivetran_synced    
       
                 
from parcelperform.shipments s 
)
select source, shipment_uuid, pp_created_date, pp_updated_date, shopify_order_datetime, tracking_number, carrier_reference, 
        shopify_order_nr, carrier, status, weight_grams, phase_key, current_phase, webstore, quantity, email, amount, currency, 
        case when transf_cte.country = 'United Kingdom of Great Britain and Northern Ireland' then 'United Kingdom'
        when transf_cte.country = 'United States of America' then 'United States'
	else transf_cte.country
	end as country, 
        city, address, postal_code, first_name, last_name, payment_type, _fivetran_synced
from transf_cte