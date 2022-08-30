--Author: Etoma Egot
   --Last modified: 2022-03-05
   --Change: Added a hash key to the officevibe engagement table as it was generally difficult to compare this table between redshift and postgres
   
   
select 
     md5(cast(coalesce(cast(date as varchar), '') || '-' || coalesce(cast(group_name as varchar), '') || '-' || coalesce(cast(metric_id as varchar), '') || '-' || coalesce(cast(_fivetran_index as varchar), '') as varchar)) as hash_id,
     date,
     group_name,
     metric_id,
     metric_name,
     metric_value,
     _fivetran_batch,
     _fivetran_index,
     _fivetran_synced
    
from
"airup_eu_dwh"."officevibe"."engagement"

 
        where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."officevibe"."dim_engagement")
        