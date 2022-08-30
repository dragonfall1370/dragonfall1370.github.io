 



SELECT 
    email,
    id,
    custom_nps_1,
    custom_nps_2,
    country,
    custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    custom_ucg_1,
    custom_ucg_2,
    custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    NULL AS custom_first_active,
	NULL AS custom_last_active,
	NULL AS custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'ch'::text AS shop
FROM "airup_eu_dwh"."klaviyo_ch"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    NULL AS custom_nps_2,
    country,
    custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    custom_ucg_1,
    custom_ucg_2,
    custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    CASE 
        WHEN custom_first_active LIKE '%-%-%' THEN TO_DATE(custom_first_active, 'YYYY-MM-DD')
        WHEN custom_first_active like '%/%/%' THEN TO_DATE(custom_first_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_active,
    CASE 
        WHEN custom_last_active LIKE '%-%-%' THEN TO_DATE(custom_last_active, 'YYYY-MM-DD')
        WHEN custom_last_active like '%/%/%' THEN TO_DATE(custom_last_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_last_active,
	custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'fr'::text AS shop
FROM "airup_eu_dwh"."klaviyo_fr"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    custom_nps_2,
    country,
    custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    custom_ucg_1,
    custom_ucg_2,
    custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    CASE 
        WHEN custom_first_active LIKE '%-%-%' THEN TO_DATE(custom_first_active, 'YYYY-MM-DD')
        WHEN custom_first_active like '%/%/%' THEN TO_DATE(custom_first_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_active,
    CASE 
        WHEN custom_last_active LIKE '%-%-%' THEN TO_DATE(custom_last_active, 'YYYY-MM-DD')
        WHEN custom_last_active like '%/%/%' THEN TO_DATE(custom_last_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_last_active,
	custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'it'::text AS shop
FROM "airup_eu_dwh"."klaviyo_it"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    custom_nps_2,
    country,
    custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    custom_ucg_1,
    custom_ucg_2,
    custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    CASE 
        WHEN custom_first_active LIKE '%-%-%' THEN TO_DATE(custom_first_active, 'YYYY-MM-DD')
        WHEN custom_first_active like '%/%/%' THEN TO_DATE(custom_first_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_active,
    CASE 
        WHEN custom_last_active LIKE '%-%-%' THEN TO_DATE(custom_last_active, 'YYYY-MM-DD')
        WHEN custom_last_active like '%/%/%' THEN TO_DATE(custom_last_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_last_active,
	custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'nl'::text AS shop
FROM "airup_eu_dwh"."klaviyo_nl"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    custom_nps_2,
    country,
    custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    custom_ucg_1,
    custom_ucg_2,
    custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    CASE 
        WHEN custom_first_active LIKE '%-%-%' THEN TO_DATE(custom_first_active, 'YYYY-MM-DD')
        WHEN custom_first_active like '%/%/%' THEN TO_DATE(custom_first_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_active,
    CASE 
        WHEN custom_last_active LIKE '%-%-%' THEN TO_DATE(custom_last_active, 'YYYY-MM-DD')
        WHEN custom_last_active like '%/%/%' THEN TO_DATE(custom_last_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_last_active,
	custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'uk'::text AS shop
FROM "airup_eu_dwh"."klaviyo_uk"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    custom_nps_2,
    country,
    CASE 
        WHEN custom_first_purchase_date LIKE '%-%-%' THEN TO_DATE(custom_first_purchase_date, 'YYYY-MM-DD')
        WHEN custom_first_purchase_date like '%/%/%' THEN TO_DATE(custom_first_purchase_date, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    custom_ucg_1,
    custom_ucg_2,
    CASE 
        WHEN custom_email_opt_in_date LIKE '%-%-%' THEN TO_DATE(custom_email_opt_in_date, 'YYYY-MM-DD')
        WHEN custom_email_opt_in_date like '%/%/%' THEN TO_DATE(custom_email_opt_in_date, 'DD/MM/YYYY')
        ELSE null
    END AS custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    CASE 
        WHEN custom_first_active LIKE '%-%-%' THEN TO_DATE(custom_first_active, 'YYYY-MM-DD')
        WHEN custom_first_active like '%/%/%' THEN TO_DATE(custom_first_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_active,
    CASE 
        WHEN custom_last_active LIKE '%-%-%' THEN TO_DATE(custom_last_active, 'YYYY-MM-DD')
        WHEN custom_last_active like '%/%/%' THEN TO_DATE(custom_last_active, 'DD/MM/YYYY')
        ELSE null
    END AS custom_last_active,
	custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'gmbh'::text AS shop
FROM "airup_eu_dwh"."klaviyo"."person"
    -----incremental table macro---
  
  where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."klaviyo_global"."dim_person")
  

  UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    NULL AS custom_nps_2,
    country,
    custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    NULL AS custom_ucg_1,
    custom_ucg_2,
    custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    NULL AS custom_stamp_card_group,
    NULL AS custom_stamp_control,
    NULL AS custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    NULL AS custom_first_active,
	NULL AS custom_last_active,
	NULL AS custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'se'::text AS shop
FROM "airup_eu_dwh"."klaviyo_se"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    custom_nps_2,
    country,
    CASE 
        WHEN custom_first_purchase_date LIKE '%-%-%' THEN TO_DATE(custom_first_purchase_date, 'YYYY-MM-DD')
        WHEN custom_first_purchase_date like '%/%/%' THEN TO_DATE(custom_first_purchase_date, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    NULL AS custom_ucg_1,
    custom_ucg_2,
    CASE 
        WHEN custom_email_opt_in_date LIKE '%-%-%' THEN TO_DATE(custom_email_opt_in_date, 'YYYY-MM-DD')
        WHEN custom_email_opt_in_date like '%/%/%' THEN TO_DATE(custom_email_opt_in_date, 'DD/MM/YYYY')
        ELSE null
    END AS custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    custom_stamp_card_group,
    custom_stamp_control,
    custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    NULL AS custom_first_active,
	NULL AS custom_last_active,
	NULL AS custom_initial_source,
    custom_sms_consent,
    custom_sms_consent_date,
    'at'::text AS shop
FROM "airup_eu_dwh"."klaviyo_at"."person"

UNION ALL

SELECT
    email,
    id,
    custom_nps_1,
    NULL AS custom_nps_2,
    country,
    CASE 
        WHEN custom_first_purchase_date LIKE '%-%-%' THEN TO_DATE(custom_first_purchase_date, 'YYYY-MM-DD')
        WHEN custom_first_purchase_date like '%/%/%' THEN TO_DATE(custom_first_purchase_date, 'DD/MM/YYYY')
        ELSE null
    END AS custom_first_purchase_date,
    custom_accepts_marketing,
    created,
    NULL AS custom_ucg_1,
    NULL AS custom_ucg_2,
    CASE 
        WHEN custom_email_opt_in_date LIKE '%-%-%' THEN TO_DATE(custom_email_opt_in_date, 'YYYY-MM-DD')
        WHEN custom_email_opt_in_date like '%/%/%' THEN TO_DATE(custom_email_opt_in_date, 'DD/MM/YYYY')
        ELSE null
    END AS custom_email_opt_in_date,
    custom_consent,
    _fivetran_synced,
    custom_consent_timestamp,
    NULL AS custom_stamp_card_group,
    NULL AS custom_stamp_control,
    NULL AS custom_stamp_amount,
    custom_language,
    first_name,
    last_name,
    address_1,
	address_2,
	city,
	country,
	zip,
    custom_source,
    NULL AS custom_first_active,
	NULL AS custom_last_active,
	NULL AS custom_initial_source,
    NULL AS custom_sms_consent,
    NULL AS custom_sms_consent_date,
    'us'::text AS shop
FROM "airup_eu_dwh"."klaviyo_us"."person"