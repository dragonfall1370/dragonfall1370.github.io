

SELECT apdr.date,
    am.country_fullname,
    am.country_grouping,
    am.country_abbreviation AS region,
    'SEA'::text AS channel,
    'SEA'::text AS channel_subcategory,
    sum(apdr.spend) AS total_spend
   FROM "airup_eu_dwh"."bingads"."ad_performance_daily_report" apdr
     LEFT JOIN "airup_eu_dwh"."bingads"."fct_bing_account_mapping" am ON apdr.account_id = am.account_id
  GROUP BY apdr.date, am.country_fullname, am.country_grouping, am.country_abbreviation, 'SEA'::text
  ORDER BY apdr.date DESC