

SELECT 
    to_timestamp(to_date(substring(date_hour_minute,1,8),'YYYYMMDD') ||' '|| (substring(date_hour_minute,9,12)||'00')::time, 'YYYY-MM-DD HH24:MI:SS', TRUE)::timestamp AS date
    , sessions
    , user_type
    , transactions
    , csam.country_grouping AS region
    , csam.country_fullname AS country
FROM "airup_eu_dwh"."google_analytics_hourly"."conversion_rate_hourly"
LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" csam ON profile in  (ltrim(rtrim(csam.google_analytics_profiles, '}'), '{'))
WHERE profile <> '224409113'