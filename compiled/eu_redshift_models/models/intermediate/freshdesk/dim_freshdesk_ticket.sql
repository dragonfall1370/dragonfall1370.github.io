-----Author: Etoma Egot
-----Last Modified By: Nham on 2022-05-18
---Change: remove the where condition since it remove some categories which should still be remained in the models. I created a new table dim_freshdes_ticket_excl_some_categories
---to remove unncessary categories )
   
 
select
    * from "airup_eu_dwh"."freshdesk"."ticket"


 --