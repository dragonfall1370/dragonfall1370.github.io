


WITH campaign AS (
         SELECT cr.date,
            csam.country_fullname,
            csam.country_grouping,
            csam.country_abbreviation as region,
            cr.account_id,
            'Paid Social'::text AS channel,
            cr.ad_id,
            cr.campaign_name,
                CASE
                    WHEN cr.campaign_name::text ~~ '%-generic_%'::text THEN 'Generic'::text
                    WHEN cr.campaign_name::text ~~ '%-retention_%'::text THEN 'Retention'::text
                    WHEN cr.campaign_name::text ~~ '%-cus_%'::text THEN 'Customer'::text
                    WHEN cr.campaign_name::text ~~ '%-retargeting_%'::text OR cr.campaign_name::text ~~ '%s-ret_%'::text THEN 'Retargeting'::text
                    WHEN cr.campaign_name::text ~~ '%-brand_%'::text THEN 'Brand'::text
                    WHEN cr.campaign_name::text ~~ '%-prospecting_%'::text OR cr.campaign_name::text ~~ '%s-pro_%'::text THEN 'Prospecting'::text
                    ELSE 'Other'::text
                END AS campaign_type,
            sum(cr.spend) AS cost
           FROM "airup_eu_dwh"."facebook_ads"."fct_custom_conversion_value_report_enriched" cr
                 LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" csam ON cr.account_id = ltrim(rtrim(csam.facebook_account_ids, '}'), '{')::int8
          GROUP BY 1,2,3,4,5,6,7,8,9
        ), count_order AS (
         SELECT balpa.date,
            balpa.ad_id,
            sum(balpa.value) AS orders
           FROM "airup_eu_dwh"."facebook_ads"."custom_conversion_report_actions" balpa
          WHERE balpa.action_type::text = 'purchase'::text
          GROUP BY balpa.date, balpa.ad_id
        ), purchases AS (
         SELECT ccvrav.date,
            ccvrav.ad_id,
            sum(ccvrav.value) AS purchases
           FROM  "airup_eu_dwh"."facebook_ads"."custom_conversion_report_action_values" ccvrav
          WHERE ccvrav.action_type::text = 'offsite_conversion.fb_pixel_purchase'::text
          GROUP BY ccvrav.date, ccvrav.ad_id
          ORDER BY ccvrav.date DESC
        )
 SELECT campaign.date,
    campaign.region,
    campaign.account_id,
    campaign.channel,
    campaign.ad_id,
    campaign.campaign_name,
    campaign.campaign_type,
    purchases.purchases AS conversions,
    campaign.cost,
    count_order.orders
   FROM campaign
     LEFT JOIN count_order ON campaign.date = count_order.date AND campaign.ad_id::text = count_order.ad_id::text
     LEFT JOIN purchases ON campaign.date = purchases.date AND campaign.ad_id::text = purchases.ad_id::text
  ORDER BY campaign.date DESC, campaign.region, campaign.ad_id