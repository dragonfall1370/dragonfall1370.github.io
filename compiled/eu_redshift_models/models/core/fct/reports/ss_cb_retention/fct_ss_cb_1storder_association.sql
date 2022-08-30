---Authors: Nham Dao
---Last Modified by: Nham Dao
 --/*#################################################################
--this view prepares data for the first order association analysis of customers who bought starter set or
--colored bottle in their first order
--#################################################################*/
 
 
 WITH first_order_with_charcoal_ss AS
  (SELECT starter_set_and_color_bottle_retention_summary_data_1.customer_id
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
   WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1
     AND (starter_set_and_color_bottle_retention_summary_data_1.sku in ('140000015',
                                                                        '100000040',
                                                                        '140000016',
                                                                        '140000017',
                                                                        '100000033',
                                                                        '100000005',
                                                                        'a-1000096',
                                                                        '100000002',
                                                                        '100000055',
                                                                        '100000003',
                                                                        '100000004',
                                                                        '140000014'))
   GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id),

first_order_with_white_ss AS
  (SELECT starter_set_and_color_bottle_retention_summary_data_1.customer_id
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
   WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1
     AND (starter_set_and_color_bottle_retention_summary_data_1.sku in ('140000023',
                                                                        '140000024',
                                                                        '100000007',
                                                                        '100000041',
                                                                        '140000021',
                                                                        '140000020',
                                                                        '100000056',
                                                                        '100000008',
                                                                        '140000022',
                                                                        '100000006'))
   GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id),

first_order_with_hot_pink_cb AS
  (SELECT starter_set_and_color_bottle_retention_summary_data_1.customer_id
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
   WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1
     AND starter_set_and_color_bottle_retention_summary_data_1.sku::text = '140000010'::text
   GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id),

first_order_with_ocean_blue_cb AS
  (SELECT starter_set_and_color_bottle_retention_summary_data_1.customer_id
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
   WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1
     AND starter_set_and_color_bottle_retention_summary_data_1.sku::text = '140000011'::text
   GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id),

first_order_with_electric_orange_cb AS
  (SELECT starter_set_and_color_bottle_retention_summary_data_1.customer_id
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
   WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1
     AND starter_set_and_color_bottle_retention_summary_data_1.sku::text = '140000009'::text
   GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id),

first_order AS
  (SELECT starter_set_and_color_bottle_retention_summary_data.customer_id,
          starter_set_and_color_bottle_retention_summary_data.created_at::date,
          starter_set_and_color_bottle_retention_summary_data.country_fullname,
          starter_set_and_color_bottle_retention_summary_data.subcategory_1
   FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data
   WHERE rank_order = 1),

charcoal_ss_first_order_associate AS
  (SELECT 'Charcoal Starter Set'::text AS main_category,
          first_order.customer_id,
          first_order.created_at::date AS order_date,
          first_order.country_fullname,
          first_order.subcategory_1,
          count(*) AS order_count
   FROM first_order
   WHERE customer_id in
       (SELECT customer_id
        FROM first_order_with_charcoal_ss)
   GROUP BY first_order.customer_id,
            first_order.created_at::date,
            first_order.country_fullname,
            first_order.subcategory_1),

white_ss_first_order_associate AS
  (SELECT 'White Starter Set'::text AS main_category,
          first_order.customer_id,
          first_order.created_at::date AS order_date,
          first_order.country_fullname,
          first_order.subcategory_1,
          count(*) AS order_count
   FROM first_order
   WHERE customer_id in
       (SELECT customer_id
        FROM first_order_with_white_ss)
   GROUP BY first_order.customer_id,
            first_order.created_at::date,
            first_order.country_fullname,
            first_order.subcategory_1),

hot_pink_cb_first_order_associate AS
  (SELECT 'Hot Pink Color Bottle'::text AS main_category,
          first_order.customer_id,
          first_order.created_at::date AS order_date,
          first_order.country_fullname,
          first_order.subcategory_1,
          count(*) AS order_count
   FROM first_order
   WHERE customer_id in
       (SELECT customer_id
        FROM first_order_with_hot_pink_cb)
   GROUP BY first_order.customer_id,
            first_order.created_at::date,
            first_order.country_fullname,
            first_order.subcategory_1),

ocean_blue_cb_first_order_associate AS
  (SELECT 'Ocean Blue Color Bottle'::text AS main_category,
          first_order.customer_id,
          first_order.created_at::date AS order_date,
          first_order.country_fullname,
          first_order.subcategory_1,
          count(*) AS order_count
   FROM first_order
   WHERE customer_id in
       (SELECT customer_id
        FROM first_order_with_ocean_blue_cb)
   GROUP BY first_order.customer_id,
            first_order.created_at::date,
            first_order.country_fullname,
            first_order.subcategory_1),

electric_orange_cb_first_order_associate AS
  (SELECT 'Electric Orange Color Bottle'::text AS main_category,
          first_order.customer_id,
          first_order.created_at::date AS order_date,
          first_order.country_fullname,
          first_order.subcategory_1,
          count(*) AS order_count
   FROM first_order
   WHERE customer_id in
       (SELECT customer_id
        FROM first_order_with_electric_orange_cb)
   GROUP BY first_order.customer_id,
            first_order.created_at::date,
            first_order.country_fullname,
            first_order.subcategory_1)
SELECT *
FROM charcoal_ss_first_order_associate
UNION
SELECT *
FROM white_ss_first_order_associate
UNION
SELECT *
FROM hot_pink_cb_first_order_associate
UNION
SELECT *
FROM ocean_blue_cb_first_order_associate
UNION
SELECT *
FROM electric_orange_cb_first_order_associate