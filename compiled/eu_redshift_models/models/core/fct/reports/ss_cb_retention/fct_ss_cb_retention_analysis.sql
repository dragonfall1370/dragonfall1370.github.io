---Authors: Nham Dao
---Last Modified by: Nham Dao
 --/*#################################################################
--this view prepares data for retention analysis of starter set and colored bottle (only consider customers who
--bought exactly 1 starter set/colored bottle in their first purchase)
--#################################################################*/
 
 
WITH distinct_customer_second_order AS
  (SELECT summary.customer_id,
          summary.country_fullname,
          summary.created_at,
          summary.timediff
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" SUMMARY
   WHERE summary.rank_order = 2
   GROUP BY summary.customer_id,
            summary.country_fullname,
            summary.created_at,
            summary.timediff),
charcoal_ss_first_order AS
  (SELECT summary_1.customer_id,
          summary_1.created_at::date AS order_date
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary_1
   WHERE summary_1.rank_order = 1
     AND (summary_1.sku::text in ('140000015',
                                  '100000040',
                                  '140000016',
                                  '140000017',
                                  '100000033',
                                  '100000005' ,
                                  'a-1000096',
                                  '100000002',
                                  '100000055',
                                  '100000003',
                                  '100000004',
                                  '140000014'))
   GROUP BY summary_1.customer_id, (summary_1.created_at::date)),
charcoal_ss_retention AS
  (SELECT 'Charcoal Starter Set'::text AS category,
          summary.country_fullname,
          charcoal.order_date AS first_order_date,
          count(charcoal.customer_id) AS number_of_retention,
          sum(CASE
                  WHEN summary.timediff <= 7::double precision THEN 1
                  ELSE 0
              END) AS "7_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 30::double precision THEN 1
                  ELSE 0
              END) AS "30_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 60::double precision THEN 1
                  ELSE 0
              END) AS "60_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 90::double precision THEN 1
                  ELSE 0
              END) AS "90_days_retention"
   FROM distinct_customer_second_order SUMMARY
   LEFT JOIN charcoal_ss_first_order charcoal ON summary.customer_id = charcoal.customer_id
   WHERE charcoal.customer_id IS NOT NULL
   GROUP BY summary.country_fullname,
            charcoal.order_date),
white_ss_first_order AS
  (SELECT summary_2.customer_id,
          summary_2.created_at::date AS order_date
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary_2
   WHERE summary_2.rank_order = 1
     AND (summary_2.sku in ('140000023',
                            '140000024',
                            '100000007',
                            '100000041',
                            '140000021',
                            '140000020',
                            '100000056',
                            '100000008',
                            '140000022',
                            '100000006'))
   GROUP BY summary_2.customer_id, (summary_2.created_at::date)),
white_ss_retention AS
  (SELECT 'White Starter Set'::text AS category,
          summary.country_fullname,
          white.order_date AS first_order_date,
          count(white.customer_id) AS number_of_retention,
          sum(CASE
                  WHEN summary.timediff <= 7::double precision THEN 1
                  ELSE 0
              END) AS "7_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 30::double precision THEN 1
                  ELSE 0
              END) AS "30_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 60::double precision THEN 1
                  ELSE 0
              END) AS "60_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 90::double precision THEN 1
                  ELSE 0
              END) AS "90_days_retention"
   FROM distinct_customer_second_order SUMMARY
   LEFT JOIN white_ss_first_order white ON summary.customer_id = white.customer_id
   WHERE white.customer_id IS NOT NULL
   GROUP BY summary.country_fullname,
            white.order_date),
hot_pink_cb_first_order AS
  (SELECT summary_3.customer_id,
          summary_3.created_at::date AS order_date
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary_3
   WHERE summary_3.rank_order = 1
     AND summary_3.sku::text = '140000010'::text
   GROUP BY summary_3.customer_id, (summary_3.created_at::date)),
                                       hot_pink_cb_retention AS
  (SELECT 'Hot Pink Color Bottle'::text AS category,
          summary.country_fullname,
          hot_pink.order_date AS first_order_date,
          count(hot_pink.customer_id) AS number_of_retention,
          sum(CASE
                  WHEN summary.timediff <= 7::double precision THEN 1
                  ELSE 0
              END) AS "7_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 30::double precision THEN 1
                  ELSE 0
              END) AS "30_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 60::double precision THEN 1
                  ELSE 0
              END) AS "60_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 90::double precision THEN 1
                  ELSE 0
              END) AS "90_days_retention"
   FROM distinct_customer_second_order SUMMARY
   LEFT JOIN hot_pink_cb_first_order hot_pink ON summary.customer_id = hot_pink.customer_id
   WHERE hot_pink.customer_id IS NOT NULL
   GROUP BY summary.country_fullname,
            hot_pink.order_date),
ocean_blue_cb_first_order AS
  (SELECT summary_4.customer_id,
          summary_4.created_at::date AS order_date
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary_4
   WHERE summary_4.rank_order = 1
     AND summary_4.sku::text = '140000011'::text
   GROUP BY summary_4.customer_id, (summary_4.created_at::date)),
ocean_blue_cb_retention AS
  (SELECT 'Ocean Blue Color Bottle'::text AS category,
          summary.country_fullname,
          ocean_blue.order_date AS first_order_date,
          count(ocean_blue.customer_id) AS number_of_retention,
          sum(CASE
                  WHEN summary.timediff <= 7::double precision THEN 1
                  ELSE 0
              END) AS "7_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 30::double precision THEN 1
                  ELSE 0
              END) AS "30_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 60::double precision THEN 1
                  ELSE 0
              END) AS "60_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 90::double precision THEN 1
                  ELSE 0
              END) AS "90_days_retention"
   FROM distinct_customer_second_order SUMMARY
   LEFT JOIN ocean_blue_cb_first_order ocean_blue ON summary.customer_id = ocean_blue.customer_id
   WHERE ocean_blue.customer_id IS NOT NULL
   GROUP BY summary.country_fullname,
            ocean_blue.order_date),
electric_orange_cb_first_order AS
  (SELECT summary_4.customer_id,
          summary_4.created_at::date AS order_date
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary_4
   WHERE summary_4.rank_order = 1
     AND summary_4.sku::text = '140000009'::text
   GROUP BY summary_4.customer_id, (summary_4.created_at::date)),
                                       electric_orange_cb_retention AS
  (SELECT 'Electric Orange Color Bottle'::text AS category,
          summary.country_fullname,
          electric_orange.order_date AS first_order_date,
          count(electric_orange.customer_id) AS number_of_retention,
          sum(CASE
                  WHEN summary.timediff <= 7::double precision THEN 1
                  ELSE 0
              END) AS "7_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 30::double precision THEN 1
                  ELSE 0
              END) AS "30_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 60::double precision THEN 1
                  ELSE 0
              END) AS "60_days_retention",
          sum(CASE
                  WHEN summary.timediff <= 90::double precision THEN 1
                  ELSE 0
              END) AS "90_days_retention"
   FROM distinct_customer_second_order SUMMARY
   LEFT JOIN electric_orange_cb_first_order electric_orange ON summary.customer_id = electric_orange.customer_id
   WHERE electric_orange.customer_id IS NOT NULL
   GROUP BY summary.country_fullname,
            electric_orange.order_date),
count_first_order AS
  (SELECT CASE
              WHEN summary_5.sku in ('140000015',
                                     '100000040',
                                     '140000016',
                                     '140000017',
                                     '100000033',
                                     '100000005' ,
                                     'a-1000096',
                                     '100000002',
                                     '100000055',
                                     '100000003',
                                     '100000004',
                                     '140000014') THEN 'Charcoal Starter Set'::text
              WHEN summary_5.sku in ('140000023',
                                     '140000024',
                                     '100000007',
                                     '100000041',
                                     '140000021',
                                     '140000020',
                                     '100000056',
                                     '100000008',
                                     '140000022',
                                     '100000006') THEN 'White Starter Set'::text
              WHEN summary_5.sku = '140000010' THEN 'Hot Pink Color Bottle'::text
              WHEN summary_5.sku = '140000011' THEN 'Ocean Blue Color Bottle'::text
              WHEN summary_5.sku = '140000009' THEN 'Electric Orange Color Bottle'::text
              ELSE NULL
          END AS category,
          summary_5.created_at::date AS first_order_date,
          summary_5.country_fullname,
          count(*) AS number_of_first_order
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary_5
   WHERE summary_5.rank_order = 1
     AND (summary_5.sku in ('140000015',
                                     '100000040',
                                     '140000016',
                                     '140000017',
                                     '100000033',
                                     'a-1000096',
                                     '100000002',
                                     '100000005',
                                     '100000055',
                                     '100000003',
                                     '100000004',
                                     '140000014',
                                     '140000023',
                                     '140000024',
                                     '100000007',
                                     '100000041',
                                     '140000021',
                                     '140000020',
                                     '100000056',
                                     '100000008',
                                     '140000022',
                                     '100000006',
                                     '140000010',
                                     '140000011' ,
                                     '140000009'))
   GROUP BY (CASE
                 WHEN summary_5.sku in ('140000015',
                                        '100000040',
                                        '140000016',
                                        '140000017',
                                        '100000033',
                                        '100000005' ,
                                        'a-1000096',
                                        '100000002',
                                        '100000055',
                                        '100000003',
                                        '100000004',
                                        '140000014') THEN 'Charcoal Starter Set'::text
                 WHEN summary_5.sku in ('140000023',
                                        '140000024',
                                        '100000007',
                                        '100000041',
                                        '140000021',
                                        '140000020',
                                        '100000056',
                                        '100000008',
                                        '140000022',
                                        '100000006') THEN 'White Starter Set'::text
                 WHEN summary_5.sku = '140000010' THEN 'Hot Pink Color Bottle'::text
                 WHEN summary_5.sku = '140000011' THEN 'Ocean Blue Color Bottle'::text
                 WHEN summary_5.sku = '140000009' THEN 'Electric Orange Color Bottle'::text
                 ELSE NULL
             END), (summary_5.created_at::date), summary_5.country_fullname),
all_bottle_retention AS
  (SELECT charcoal_ss_retention.category,
          charcoal_ss_retention.country_fullname,
          charcoal_ss_retention.first_order_date,
          charcoal_ss_retention.number_of_retention,
          charcoal_ss_retention."7_days_retention",
          charcoal_ss_retention."30_days_retention",
          charcoal_ss_retention."60_days_retention",
          charcoal_ss_retention."90_days_retention"
   FROM charcoal_ss_retention
   UNION SELECT white_ss_retention.category,
                white_ss_retention.country_fullname,
                white_ss_retention.first_order_date,
                white_ss_retention.number_of_retention,
                white_ss_retention."7_days_retention",
                white_ss_retention."30_days_retention",
                white_ss_retention."60_days_retention",
                white_ss_retention."90_days_retention"
   FROM white_ss_retention
   UNION SELECT hot_pink_cb_retention.category,
                hot_pink_cb_retention.country_fullname,
                hot_pink_cb_retention.first_order_date,
                hot_pink_cb_retention.number_of_retention,
                hot_pink_cb_retention."7_days_retention",
                hot_pink_cb_retention."30_days_retention",
                hot_pink_cb_retention."60_days_retention",
                hot_pink_cb_retention."90_days_retention"
   FROM hot_pink_cb_retention
   UNION SELECT ocean_blue_cb_retention.category,
                ocean_blue_cb_retention.country_fullname,
                ocean_blue_cb_retention.first_order_date,
                ocean_blue_cb_retention.number_of_retention,
                ocean_blue_cb_retention."7_days_retention",
                ocean_blue_cb_retention."30_days_retention",
                ocean_blue_cb_retention."60_days_retention",
                ocean_blue_cb_retention."90_days_retention"
   FROM ocean_blue_cb_retention
   UNION SELECT electric_orange_cb_retention.category,
                electric_orange_cb_retention.country_fullname,
                electric_orange_cb_retention.first_order_date,
                electric_orange_cb_retention.number_of_retention,
                electric_orange_cb_retention."7_days_retention",
                electric_orange_cb_retention."30_days_retention",
                electric_orange_cb_retention."60_days_retention",
                electric_orange_cb_retention."90_days_retention"
   FROM electric_orange_cb_retention)
SELECT foc.category,
       foc.first_order_date,
       foc.country_fullname,
       foc.number_of_first_order,
       abr.number_of_retention,
       abr."7_days_retention",
       abr."30_days_retention",
       abr."60_days_retention",
       abr."90_days_retention"
FROM count_first_order foc
LEFT JOIN all_bottle_retention abr ON foc.category = abr.category
AND foc.country_fullname::text = abr.country_fullname::text
AND foc.first_order_date = abr.first_order_date