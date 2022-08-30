-----Author: Etoma Egot
-----Last Modified By: Nham on 2022-05-03
---Change: remove the incremental config. The old set up (use id as primary key to incrementally update the model stop the revised information being 
---import into the model. E.g: if we change the category of a ticket, this row will not be updated since the id is the same)
   
 
 with fticket as(
    select
    * from "airup_eu_dwh"."freshdesk"."ticket"
    where id not in(
        select 
          id
              from "airup_eu_dwh"."freshdesk"."ticket"
              where lower(custom_cf_category) ='general information' and lower(custom_cf_subcategory_1) IN ('others')
              or ((lower(custom_cf_category) ='general information') and (lower(custom_cf_subcategory_1) IN ('online shop')) AND (lower(custom_cf_subcategory_2) IN ('others')))
              or ((lower(custom_cf_category) ='general information') and (lower(custom_cf_subcategory_1) IN ('online shop')) AND (lower(custom_cf_subcategory_2) IN ('information')))
              or (lower(custom_cf_category) ='chat' and lower(custom_cf_subcategory_1) IN ('chat - no reaction'))
              or lower(custom_cf_category) in ('product questions','suggestions for improvement')
        )
      )

 select * from fticket