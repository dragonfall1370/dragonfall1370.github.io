 


SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'ch' AS shop
FROM "airup_eu_dwh"."klaviyo_ch"."event"

UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'fr' AS shop
FROM "airup_eu_dwh"."klaviyo_fr"."event"

UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'it' AS shop
FROM "airup_eu_dwh"."klaviyo_it"."event"

UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'nl' AS shop
FROM "airup_eu_dwh"."klaviyo_nl"."event"

UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'uk' AS shop
FROM "airup_eu_dwh"."klaviyo_uk"."event"

UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'gmbh' AS shop
FROM "airup_eu_dwh"."klaviyo"."event"


UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'se' AS shop
FROM "airup_eu_dwh"."klaviyo_se"."event"

UNION ALL

SELECT id,
    "datetime",
    campaign_id,
    property_value,
    flow_id,
    flow_message_id,
    property_campaign_name,
    person_id,
    type,
    _fivetran_synced,
    'at' AS shop
FROM "airup_eu_dwh"."klaviyo_at"."event"
    -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."klaviyo_global"."dim_event")
  