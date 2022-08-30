 

SELECT 
*
FROM "airup_eu_dwh"."klaviyo_ch"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_fr"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_it"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_nl"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_uk"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_se"."campaign"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_at"."campaign"

    -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."klaviyo_global"."dim_campaign")
  