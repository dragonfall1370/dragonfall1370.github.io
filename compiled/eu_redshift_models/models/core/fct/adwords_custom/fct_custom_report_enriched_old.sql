-- changed source for adwords_custom from fct to raw table (Abhishek Pathak, 10-Mar-2022)


WITH NS AS (
SELECT
	1 AS n
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
UNION ALL
SELECT 5
UNION ALL
SELECT 6
UNION ALL
SELECT 7
UNION ALL
SELECT 8
UNION ALL
SELECT 9
UNION ALL
SELECT 10
), countries AS (
  SELECT
	country_system_account_mapping.country_fullname,
  country_system_account_mapping.country_grouping,
	TRIM(SPLIT_PART(ltrim(rtrim(country_system_account_mapping.adwords_customer_ids , '}'), '{'), ',', NS.n))::int8 AS customer_ids
FROM
	NS
INNER JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON
	NS.n <= regexp_count(ltrim(rtrim(country_system_account_mapping.adwords_customer_ids , '}'), '{'),
	',') + 1
), test as (
SELECT COALESCE(countries.country_fullname, 'other') AS country_fullname,
    COALESCE(countries.country_grouping, 'other') AS country_grouping,
    account_channel_mapping.account_type,
    facr.customer_id AS customer_id,
    facr.date AS date,
    facr._fivetran_id AS _fivetran_id,
    facr.bounce_rate,
    facr.impressions,
    facr.conversions,
    facr.ad_network_type,
    facr.clicks,
    facr.cost_micros / 1000000::double precision AS cost,
    facr._fivetran_synced,
    facr.name,
    facr.id
   FROM "airup_eu_dwh"."adwords_new_api"."custom_report" facr
     LEFT JOIN countries ON facr.customer_id = countries.customer_ids
     LEFT JOIN "airup_eu_dwh"."adwords"."account_mapping" account_channel_mapping ON facr.customer_id = account_channel_mapping.customer_id
)
select *
from test 
where country_fullname = 'United States'