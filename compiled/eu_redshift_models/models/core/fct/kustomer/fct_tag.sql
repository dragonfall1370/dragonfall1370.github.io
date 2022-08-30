---created_by: Nham Dao
---###################################################################################################################
        -- this view contains the ticket information for kustomer schema (it cleans up the data from the original conversation table and remove the deleted tickets)
---###################################################################################################################

select conv.created_at as created_at,conv_tag.conversation_id, conv_tag.tag_id , tg.name , source, conv.customer_country, conv.country_grouping
from "airup_eu_dwh"."kustomer"."conversation_tag" conv_tag 
left join "airup_eu_dwh"."kustomer"."tag" as tg
on conv_tag.tag_id = tg.id
left join "airup_eu_dwh"."kustomer"."dim_conversation" conv 
on conv.id = conv_tag.conversation_id
where tg.deleted is false