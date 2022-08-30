
 
 SELECT dpp.date AS date,
    date_trunc('month', dpp.date) = date_trunc('month', CURRENT_DATE) AS current_month,
    date_trunc('month', dpp.date) = date_trunc('month', CURRENT_DATE - interval '30 days') AS previous_month,
    date_trunc('quarter', dpp.date) = date_trunc('quarter', CURRENT_DATE) AS current_quarter,
    dpp.account_id AS account_id,
        CASE
            WHEN dpp.publisher_platform = 'audience_network' THEN 'facebook'
            ELSE dpp.publisher_platform
        END AS publisher_platform,
    sum(dpp.spend) AS total_spend,
        CASE
            WHEN dpp.account_id = '2313152642286471' THEN sum(dpp.spend)
            ELSE NULL
        END AS de_spend,
        CASE
            WHEN dpp.account_id = '2659498800990269' THEN sum(dpp.spend)
            ELSE NULL
        END AS fr_spend,
        CASE
            WHEN dpp.account_id = '349361226279824' THEN sum(dpp.spend)
            ELSE NULL
        END AS nl_spend,
        CASE
            WHEN dpp.account_id = '1474567866226590' THEN sum(dpp.spend)
            ELSE NULL
        END AS uk_spend,
        CASE
            WHEN dpp.account_id = '243624634144296' THEN sum(dpp.spend)
            ELSE NULL
        END AS ch_spend,
        CASE
            WHEN dpp.account_id = '844463443170677' THEN sum(dpp.spend)
            ELSE NULL
        END AS it_spend,
        CASE
            WHEN date_trunc('month', dpp.date) = (date_trunc('month', CURRENT_DATE) - interval '30 days') AND date_part('day', dpp.date) <= (date_part('day', CURRENT_DATE) - 1) THEN 'MTD'
            ELSE NULL
        END AS mtd_qualifier
   FROM "airup_eu_dwh"."facebook"."fct_delivery_platforms_prebuilt" dpp
  GROUP BY dpp.date, dpp.account_id, (
        CASE
            WHEN dpp.publisher_platform = 'audience_network' THEN 'facebook'
            ELSE dpp.publisher_platform
        END)