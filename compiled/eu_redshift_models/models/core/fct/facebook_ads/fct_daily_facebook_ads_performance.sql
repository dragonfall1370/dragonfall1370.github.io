


WITH data_pre AS (
         SELECT csam.country_grouping,
            csam.country_fullname,
            csam.country_abbreviation AS region,
            ccvr.date,
            ccvr.ad_id,
            ccvr.campaign_name,
            sum(ccvr.impressions):: numeric AS impressions,
            sum(ccvr.inline_link_clicks):: numeric AS click,
            sum(ccvr.spend) AS cost,
            sum(ccvr.reach):: numeric AS reach,
            avg(ccvr.frequency) AS frequency,
            ccvr.objective
           FROM "airup_eu_dwh"."facebook_ads"."fct_custom_conversion_value_report_enriched" ccvr
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" csam ON ccvr.account_id::text = ltrim(rtrim(csam.facebook_account_ids, '}'), '{')::int8
          GROUP BY csam.country_grouping, csam.country_fullname, csam.country_abbreviation, ccvr.date, ccvr.ad_id, ccvr.campaign_name, ccvr.reach, ccvr.frequency, ccvr.objective
          ORDER BY csam.country_abbreviation DESC, csam.country_fullname
        ), order_number AS (
         SELECT dfacvo.date,
            dfacvo.ad_id,
            dfacvo.country_fullname,
            dfacvo.region,
            dfacvo.orders
           FROM "airup_eu_dwh"."facebook_ads"."fct_daily_facebook_ads_conversion_value_order" dfacvo
        ), conversion_value_purchases AS (
         SELECT dfacvp.date,
            dfacvp.ad_id,
            dfacvp.country_fullname,
            dfacvp.region,
            dfacvp.conversion_value_purchases
           FROM "airup_eu_dwh"."facebook_ads"."fct_daily_facebook_ads_conversion_value_purchase" dfacvp
        )
 SELECT data_pre.country_grouping,
    data_pre.country_fullname,
    data_pre.region,
    data_pre.date,
    data_pre.ad_id,
    data_pre.campaign_name,
    data_pre.impressions:: numeric,
    data_pre.click:: numeric,
    data_pre.cost,
    order_number.orders,
    conversion_value_purchases.conversion_value_purchases AS sales,
    data_pre.reach:: numeric,
    data_pre.frequency,
    data_pre.objective
   FROM data_pre
     LEFT JOIN order_number ON data_pre.country_fullname::text = order_number.country_fullname AND data_pre.date = order_number.date AND data_pre.ad_id::text = order_number.ad_id::text
     LEFT JOIN conversion_value_purchases ON data_pre.country_fullname::text = conversion_value_purchases.country_fullname AND data_pre.date = conversion_value_purchases.date AND data_pre.ad_id::text = conversion_value_purchases.ad_id::text
  ORDER BY data_pre.date DESC, data_pre.country_fullname