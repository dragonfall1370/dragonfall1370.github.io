
select 
     md5(cast(coalesce(cast(date as varchar), '') || '-' || coalesce(cast(account_id as varchar), '') || '-' || coalesce(cast(publisher_platform as varchar), '') as varchar)) as hash_id,
     date,
     account_id,
     publisher_platform,
     index,
     inline,
     value,
     _1_d_view,
     _7_d_click,
     action_type,
     _fivetran_synced
    
from
"airup_eu_dwh"."facebook"."delivery_platforms_prebuilt_actions"

 
        where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."facebook"."fct_delivery_platforms_prebuilt_actions")
        