
 
with final as  (

  select 
        md5(cast(coalesce(cast(snapshot_week as varchar), '') || '-' || coalesce(cast(customer_type as varchar), '') || '-' || coalesce(cast(country as varchar), '') as varchar)) as hash_id,
        snapshot_week::date as "week", 
        customer_type,
        country,
        "share",
        total_share_per_country as total_share 
  from 
       "airup_eu_dwh"."crm"."fct_xscore_weekly_country_snapshot"

  
union

  select 
        hash_id,
        week::date,
        customer_type,
        country,
        share,
        total_share
   from 
    "airup_eu_dwh"."crm"."dim_retention_weekly_snapshots"

)

 select * from final 

 -----incrememntal table macro---
    
   where week >= (select max(week) from "airup_eu_dwh"."crm"."dim_retention_cycle_weekly_snapshots")
  