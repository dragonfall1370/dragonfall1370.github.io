 

SELECT 
*,
'ch' AS shop
FROM "airup_eu_dwh"."klaviyo_ch"."flow"

UNION ALL

SELECT
*,
'fr' AS shop
FROM "airup_eu_dwh"."klaviyo_fr"."flow"

UNION ALL

SELECT
*,
'it' AS shop
FROM "airup_eu_dwh"."klaviyo_it"."flow"

UNION ALL

SELECT
*,
'nl'AS shop
FROM "airup_eu_dwh"."klaviyo_nl"."flow"

UNION ALL

SELECT
*,
'uk' AS shop
FROM "airup_eu_dwh"."klaviyo_uk"."flow"

UNION ALL

SELECT
*,
'gmbh' AS shop
FROM "airup_eu_dwh"."klaviyo"."flow"

UNION ALL

SELECT
*,
'se' AS shop
FROM "airup_eu_dwh"."klaviyo_se"."flow"

UNION ALL

SELECT
*,
'at' AS shop
FROM "airup_eu_dwh"."klaviyo_at"."flow"

    -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."klaviyo_global"."dim_flow")
  