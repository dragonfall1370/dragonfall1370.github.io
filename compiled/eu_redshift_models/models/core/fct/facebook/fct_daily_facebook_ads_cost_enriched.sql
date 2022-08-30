

SELECT COALESCE(country_system_account_mapping.country_fullname, 'other') AS country_fullname,
    COALESCE(country_system_account_mapping.country_grouping, 'other') AS country_grouping,
    dfac.date,
    dfac.current_month,
    dfac.previous_month,
    dfac.current_quarter,
    dfac.account_id,
    dfac.publisher_platform,
    dfac.total_spend,
    dfac.de_spend,
    dfac.nl_spend,
    dfac.fr_spend,
    dfac.mtd_qualifier
   FROM "airup_eu_dwh"."facebook"."daily_facebook_ads_cost" dfac
     LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" ON dfac.account_id = ltrim(rtrim(country_system_account_mapping.facebook_account_ids , '}'), '{')::int8