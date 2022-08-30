 
 
 select 
 md5(cast(coalesce(cast(week as varchar), '') || '-' || coalesce(cast(customer_type as varchar), '') || '-' || coalesce(cast(country as varchar), '') as varchar)) as hash_id,
 "week",
 customer_type,
 country,
 share,
 total_share 
 from 
    crm.retention_cycle_weekly_snapshots
 order by week desc