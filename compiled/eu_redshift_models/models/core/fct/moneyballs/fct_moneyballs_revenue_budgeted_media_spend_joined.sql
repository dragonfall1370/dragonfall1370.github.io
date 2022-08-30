

WITH global_curr_usd AS (
	SELECT 
		date(global_currency_rates.creation_date) AS creation_date,
    	global_currency_rates.currency_abbreviation,
    	global_currency_rates.conversion_rate_eur
    FROM "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" global_currency_rates
    WHERE global_currency_rates.currency_abbreviation::text = 'USD'::text
), all_data AS ( 
    SELECT pa1.order_date,
    pa1.country,
    pa1.region,
    pa1.channel,
    pa1.nr2_buget_per_day,
    pa1.net_revenue_2,
    pa1.py_net_revenue_2,
    pa1.media_spend_budget_per_day / count(pa1.order_date) OVER (PARTITION BY pa1.order_date, pa1.country) AS media_spend_budget_per_day,
        CASE
            WHEN pa1.order_date <= CURRENT_DATE THEN pa1.media_spend / count(pa1.order_date) OVER (PARTITION BY pa1.order_date, pa1.country)
            ELSE 0
        END AS media_spend,
    pa1.pm_net_revenue_2
   FROM ( WITH budgeted_adj AS (
                 SELECT nr2_budgeted_daily.day_split AS order_date,
                    sum(nr2_budgeted_daily.nr2_buget_per_day) AS nr2_buget_per_day,
                    nr2_budgeted_daily.channel,
                    nr2_budgeted_daily.region,
                        CASE
                            WHEN nr2_budgeted_daily.country = 'Belgium'::text THEN 'Netherlands'::text
                            ELSE nr2_budgeted_daily.country
                        END AS country
                   FROM "airup_eu_dwh"."reports"."fct_nr2_budgeted_daily_eur" nr2_budgeted_daily
                  GROUP BY nr2_budgeted_daily.day_split, nr2_budgeted_daily.channel, nr2_budgeted_daily.region, (
                        CASE
                            WHEN nr2_budgeted_daily.country = 'Belgium'::text THEN 'Netherlands'::text
                            ELSE nr2_budgeted_daily.country
                        END)
                ), media_spend_budgeted_adj AS (
                 SELECT media_spend_budgeted_daily.day_split AS order_date,
                    CASE
                            WHEN media_spend_budgeted_daily.country = 'Belgium'::text THEN 'Netherlands'::text
                            ELSE media_spend_budgeted_daily.country
                        END AS country,
                    media_spend_budgeted_daily.region,
                    sum(media_spend_budgeted_daily.media_spend_budget_per_day) AS media_spend_budget_per_day
                   FROM "airup_eu_dwh"."reports"."fct_media_spend_budgeted_daily_eur" media_spend_budgeted_daily
                  GROUP BY media_spend_budgeted_daily.day_split, CASE
                            WHEN media_spend_budgeted_daily.country = 'Belgium'::text THEN 'Netherlands'::text
                            ELSE media_spend_budgeted_daily.country
                        END, media_spend_budgeted_daily.region
                ), revenue_adj AS (
                 SELECT revenue_moneyballs_all_channels.order_date,
                        CASE
                            WHEN revenue_moneyballs_all_channels.country = 'Belgium'::text THEN 'Netherlands'::text
                            WHEN revenue_moneyballs_all_channels.country = 'other'::text THEN 'Germany'::text
                            ELSE revenue_moneyballs_all_channels.country
                        END AS country,
                        CASE
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'D2C'::text THEN 'Webshop D2C'::text
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'Amazon'::text THEN 'MarketPlace'::text
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'BOL'::text THEN 'MarketPlace'::text
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'Offline Sales Excel'::text THEN 'Offline'::text
                            ELSE NULL::text
                        END AS channel,
                        CASE
                            WHEN revenue_moneyballs_all_channels.region = 'other'::text THEN 'Central Europe'::text
                            ELSE revenue_moneyballs_all_channels.region
                        END AS region,
                    sum(revenue_moneyballs_all_channels.cy_net_revenue_2) AS net_revenue_2,
                    sum(revenue_moneyballs_all_channels.py_net_revenue_2) AS py_net_revenue_2,
                    sum(revenue_moneyballs_all_channels.pm_net_revenue_2) AS pm_net_revenue_2
                    FROM "airup_eu_dwh"."reports"."fct_revenue_moneyballs_all_channels" revenue_moneyballs_all_channels
                  GROUP BY revenue_moneyballs_all_channels.order_date, (
                        CASE
                            WHEN revenue_moneyballs_all_channels.country = 'Belgium'::text THEN 'Netherlands'::text
                            WHEN revenue_moneyballs_all_channels.country = 'other'::text THEN 'Germany'::text
                            ELSE revenue_moneyballs_all_channels.country
                        END), (
                        CASE
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'D2C'::text THEN 'Webshop D2C'::text
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'Amazon'::text THEN 'MarketPlace'::text
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'BOL'::text THEN 'MarketPlace'::text
                            WHEN revenue_moneyballs_all_channels.purchase_channel = 'Offline Sales Excel'::text THEN 'Offline'::text
                            ELSE NULL::text
                        END), (
                        CASE
                            WHEN revenue_moneyballs_all_channels.region = 'other'::text THEN 'Central Europe'::text
                            ELSE revenue_moneyballs_all_channels.region
                        END)
                ), media_spend_adj AS (
                 SELECT media_spend_moneyballs.date AS order_date,
                    CASE
                            WHEN media_spend_moneyballs.country = 'Belgium'::text THEN 'Netherlands'::text
                            ELSE media_spend_moneyballs.country
                        END AS country,
                    media_spend_moneyballs.region,
                    sum(media_spend_moneyballs.media_spend) AS media_spend
                   FROM "airup_eu_dwh"."reports"."fct_media_spend_moneyballs" media_spend_moneyballs
                  GROUP BY media_spend_moneyballs.date, CASE
                            WHEN media_spend_moneyballs.country = 'Belgium'::text THEN 'Netherlands'::text
                            ELSE media_spend_moneyballs.country
                        END, media_spend_moneyballs.region
                )
         SELECT budgeted_adj.order_date,
            budgeted_adj.country,
            budgeted_adj.region,
            budgeted_adj.channel,
            budgeted_adj.nr2_buget_per_day,
            count(budgeted_adj.order_date) OVER (PARTITION BY budgeted_adj.order_date, budgeted_adj.country) AS count,
            COALESCE(media_spend_budgeted_adj.media_spend_budget_per_day, 0) AS media_spend_budget_per_day,
            COALESCE(revenue_adj.net_revenue_2, 0) AS net_revenue_2,
            COALESCE(revenue_adj.py_net_revenue_2, 0) AS py_net_revenue_2,
            COALESCE(revenue_adj.pm_net_revenue_2, 0) AS pm_net_revenue_2,
            COALESCE(media_spend_adj.media_spend, 0) AS media_spend
           FROM budgeted_adj
             LEFT JOIN revenue_adj ON budgeted_adj.order_date = revenue_adj.order_date AND budgeted_adj.country = revenue_adj.country AND budgeted_adj.region = revenue_adj.region AND budgeted_adj.channel = revenue_adj.channel
             LEFT JOIN media_spend_adj ON budgeted_adj.order_date = media_spend_adj.order_date AND budgeted_adj.country = media_spend_adj.country AND budgeted_adj.region = media_spend_adj.region
             LEFT JOIN media_spend_budgeted_adj ON budgeted_adj.order_date = media_spend_budgeted_adj.order_date AND budgeted_adj.country = media_spend_budgeted_adj.country AND budgeted_adj.region = media_spend_budgeted_adj.region) pa1
  GROUP BY pa1.order_date, pa1.country, pa1.region, pa1.channel, pa1.nr2_buget_per_day, pa1.net_revenue_2, pa1.py_net_revenue_2, pa1.media_spend, pa1.media_spend_budget_per_day, pa1.pm_net_revenue_2
  ORDER BY pa1.order_date DESC
) SELECt
 order_date
 , country
 , region
 , channel
 , nr2_buget_per_day
 , nr2_buget_per_day*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801)  AS nr2_buget_per_day_usd
  , net_revenue_2
 , net_revenue_2*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801) AS net_revenue_2_usd
   , py_net_revenue_2
 , py_net_revenue_2*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801) AS py_net_revenue_2_usd
    , media_spend
 , media_spend*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801) AS media_spend_usd
     , media_spend_budget_per_day
 , media_spend_budget_per_day*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801) AS media_spend_budget_per_day_usd
     , pm_net_revenue_2
 , pm_net_revenue_2*coalesce(global_curr_usd.conversion_rate_eur::double precision, 1.0801) AS pm_net_revenue_2_usd
FROM all_data
LEFT JOIN global_curr_usd ON all_data.order_date = global_curr_usd.creation_date