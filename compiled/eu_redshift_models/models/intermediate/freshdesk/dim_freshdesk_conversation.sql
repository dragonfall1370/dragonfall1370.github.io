-----Author: Etoma Egot
-----Last Modified By: Etoma Egot
 
 

select 
   id,
   ticket_id,
   contact_id,
   created_at,
   updated_at,
   source,
   support_email,
   from_email,
   private,
   incoming,
   _fivetran_synced
from "airup_eu_dwh"."freshdesk"."conversation"
WHERE date(created_at) between (current_date - interval '365 day') and current_date

 
   and _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."freshdesk"."dim_freshdesk_conversation")
  