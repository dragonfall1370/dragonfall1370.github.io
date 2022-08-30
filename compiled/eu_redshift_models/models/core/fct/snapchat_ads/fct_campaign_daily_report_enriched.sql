

 SELECT cdr.date::date AS date,
    cdr.campaign_id,
    am.country AS region,
    ch.ad_account_id,
    ch.name AS campaign_name,
    cdr.spend / 1000000 AS spend
   FROM "airup_eu_dwh"."snapchat_ads"."campaign_daily_report" cdr
     LEFT JOIN "airup_eu_dwh"."snapchat_ads"."campaign_history" ch ON cdr.campaign_id::text = ch.id::text
     LEFT JOIN "airup_eu_dwh"."snapchat_ads"."fct_snap_account_mapping" am ON ch.ad_account_id::text = am.id::text
  ORDER BY (cdr.date::date) DESC