 

SELECT 
*
FROM "airup_eu_dwh"."klaviyo_ch"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_fr"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_it"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_nl"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_uk"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_se"."list"

UNION ALL

SELECT
*
FROM "airup_eu_dwh"."klaviyo_at"."list"

    -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."klaviyo_global"."dim_list")
  