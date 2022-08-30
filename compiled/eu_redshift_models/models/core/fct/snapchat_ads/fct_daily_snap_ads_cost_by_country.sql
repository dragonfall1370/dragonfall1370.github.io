

 SELECT cdre.date,
    cdre.region,
    'Paid Social'::text AS channel,
    'Snapchat'::text AS channel_subcategory,
    sum(cdre.spend) AS total_spend
   FROM "airup_eu_dwh"."snapchat_ads"."fct_campaign_daily_report_enriched" cdre
  GROUP BY cdre.date, cdre.region, 'Paid Social'::text
  ORDER BY cdre.date DESC