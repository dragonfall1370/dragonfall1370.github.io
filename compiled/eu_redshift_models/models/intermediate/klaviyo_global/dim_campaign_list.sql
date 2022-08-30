 


with final as (SELECT 
*
FROM "airup_eu_dwh"."klaviyo_ch"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_fr"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_it"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_nl"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_uk"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_se"."campaign_list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_at"."campaign_list")

select 
md5(cast(coalesce(cast(campaign_id as varchar), '') || '-' || coalesce(cast(list_id as varchar), '') as varchar)) as hash_id,
final.* 
from final
 -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."klaviyo_global"."dim_campaign_list")
  