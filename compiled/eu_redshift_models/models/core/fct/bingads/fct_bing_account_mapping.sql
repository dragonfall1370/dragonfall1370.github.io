

SELECT ad_extension_detail_daily_report.account_id,
        CASE
            WHEN ad_extension_detail_daily_report.account_id = '141534989'::bigint THEN 'Germany'::text
            WHEN ad_extension_detail_daily_report.account_id = '141730692'::bigint THEN 'Italy'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628101'::bigint THEN 'Switzerland'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628154'::bigint THEN 'France'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628164'::bigint THEN 'Netherlands'::text
            WHEN ad_extension_detail_daily_report.account_id = '141631778'::bigint THEN 'United Kingdom'::text
            WHEN ad_extension_detail_daily_report.account_id = '141811931'::bigint THEN 'Austria'::text
            WHEN ad_extension_detail_daily_report.account_id = '141905561'::bigint THEN 'Belgium'::text
            WHEN ad_extension_detail_daily_report.account_id = '141832177'::bigint THEN 'Sweden'::text
            ELSE NULL::text
        END AS country_fullname,
        CASE
            WHEN ad_extension_detail_daily_report.account_id = '141534989'::bigint OR ad_extension_detail_daily_report.account_id = '141811931'::bigint OR ad_extension_detail_daily_report.account_id = '141628101'::bigint THEN 'Central Europe'::text
            WHEN ad_extension_detail_daily_report.account_id = '141730692'::bigint OR ad_extension_detail_daily_report.account_id = '141628154'::bigint THEN 'South Europe'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628164'::bigint OR ad_extension_detail_daily_report.account_id = '141631778'::bigint OR ad_extension_detail_daily_report.account_id = '141905561'::bigint OR ad_extension_detail_daily_report.account_id = '141832177'::bigint THEN 'North Europe'::text
            ELSE NULL::text
        END AS country_grouping,
        CASE
            WHEN ad_extension_detail_daily_report.account_id = '141534989'::bigint THEN 'DE'::text
            WHEN ad_extension_detail_daily_report.account_id = '141730692'::bigint THEN 'IT'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628101'::bigint THEN 'CH'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628154'::bigint THEN 'FR'::text
            WHEN ad_extension_detail_daily_report.account_id = '141628164'::bigint THEN 'NL'::text
            WHEN ad_extension_detail_daily_report.account_id = '141631778'::bigint THEN 'UK'::text
            WHEN ad_extension_detail_daily_report.account_id = '141811931'::bigint THEN 'AT'::text
            WHEN ad_extension_detail_daily_report.account_id = '141905561'::bigint THEN 'BE'::text
            WHEN ad_extension_detail_daily_report.account_id = '141832177'::bigint THEN 'SE'::text
            ELSE NULL::text
        END AS country_abbreviation
   FROM "airup_eu_dwh"."bingads"."ad_extension_detail_daily_report" ad_extension_detail_daily_report
  GROUP BY 1,2,3,4