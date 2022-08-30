---Authors: Nham Dao
---Last Modified by: Nham Dao



--/*#################################################################
--this view prepares data for the second order association analysis of customers who bought starter set or
--colored bottle in their first order
--#################################################################*/

 

WITH first_order_with_charcoal_ss AS (
         SELECT distinct starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
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
          GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
        ), first_order_with_white_ss AS (
         SELECT distinct starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
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
          GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
        ), first_order_with_hot_pink_cb AS (
         SELECT distinct starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
           FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
          WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1 AND starter_set_and_color_bottle_retention_summary_data_1.sku::text = '140000010'::text
          GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
        ), first_order_with_ocean_blue_cb AS (
         SELECT distinct starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
           FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
          WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1 AND starter_set_and_color_bottle_retention_summary_data_1.sku::text = '140000011'::text
          GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
        ), first_order_with_electric_orange_cb AS (
         SELECT distinct starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
           FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" starter_set_and_color_bottle_retention_summary_data_1
          WHERE starter_set_and_color_bottle_retention_summary_data_1.rank_order = 1 AND starter_set_and_color_bottle_retention_summary_data_1.sku::text = '140000009'::text
          GROUP BY starter_set_and_color_bottle_retention_summary_data_1.customer_id, created_at::date
        ), second_order AS (
         SELECT summary.customer_id,
            summary.created_at::date AS created_at,
            summary.country_fullname,
            summary.subcategory_1
           FROM "airup_eu_dwh"."reports"."fct_ss_cb_retention_summary_data" summary
          WHERE summary.rank_order = 2
        ), charcoal_ss_second_order_associate AS (
         SELECT 'Charcoal Starter Set'::text AS main_category,
            second_order.customer_id,
            ss.created_at AS order_date,
            second_order.country_fullname,
            second_order.subcategory_1,
            count(*) AS order_count
           FROM second_order
           inner join first_order_with_charcoal_ss ss 
           on second_order.customer_id = ss.customer_id
          GROUP BY second_order.customer_id, ss.created_at, second_order.country_fullname, second_order.subcategory_1
        ), white_ss_second_order_associate AS (
         SELECT 'White Starter Set'::text AS main_category,
            second_order.customer_id,
            ss.created_at AS order_date,
            second_order.country_fullname,
            second_order.subcategory_1,
            count(*) AS order_count
           FROM second_order
           inner join first_order_with_white_ss ss 
           on second_order.customer_id = ss.customer_id
          GROUP BY second_order.customer_id, ss.created_at, second_order.country_fullname, second_order.subcategory_1
        ), hot_pink_cb_second_order_associate AS (
         SELECT 'Hot Pink Color Bottle'::text AS main_category,
            second_order.customer_id,
            cb.created_at AS order_date,
            second_order.country_fullname,
            second_order.subcategory_1,
            count(*) AS order_count
           FROM second_order
           inner join first_order_with_hot_pink_cb cb
           on second_order.customer_id = cb.customer_id
          GROUP BY second_order.customer_id, cb.created_at, second_order.country_fullname, second_order.subcategory_1
        ), ocean_blue_cb_second_order_associate AS (
         SELECT 'Ocean Blue Color Bottle'::text AS main_category,
            second_order.customer_id,
            cb.created_at AS order_date,
            second_order.country_fullname,
            second_order.subcategory_1,
            count(*) AS order_count
           FROM second_order   
           inner join first_order_with_ocean_blue_cb cb
           on second_order.customer_id = cb.customer_id        
          GROUP BY second_order.customer_id, cb.created_at, second_order.country_fullname, second_order.subcategory_1
        ), electric_orange_cb_second_order_associate AS (
         SELECT 'Electric Orange Color Bottle'::text AS main_category,
            second_order.customer_id,
            cb.created_at AS order_date,
            second_order.country_fullname,
            second_order.subcategory_1,
            count(*) AS order_count
           FROM second_order
           inner join first_order_with_electric_orange_cb cb
           on second_order.customer_id = cb.customer_id  
          GROUP BY second_order.customer_id, cb.created_at, second_order.country_fullname, second_order.subcategory_1
        )
 SELECT charcoal_ss_second_order_associate.main_category,
    charcoal_ss_second_order_associate.customer_id,
    charcoal_ss_second_order_associate.order_date,
    charcoal_ss_second_order_associate.country_fullname,
    charcoal_ss_second_order_associate.subcategory_1,
    charcoal_ss_second_order_associate.order_count
   FROM charcoal_ss_second_order_associate
UNION
 SELECT white_ss_second_order_associate.main_category,
    white_ss_second_order_associate.customer_id,
    white_ss_second_order_associate.order_date,
    white_ss_second_order_associate.country_fullname,
    white_ss_second_order_associate.subcategory_1,
    white_ss_second_order_associate.order_count
   FROM white_ss_second_order_associate
UNION
 SELECT hot_pink_cb_second_order_associate.main_category,
    hot_pink_cb_second_order_associate.customer_id,
    hot_pink_cb_second_order_associate.order_date,
    hot_pink_cb_second_order_associate.country_fullname,
    hot_pink_cb_second_order_associate.subcategory_1,
    hot_pink_cb_second_order_associate.order_count
   FROM hot_pink_cb_second_order_associate
UNION
 SELECT ocean_blue_cb_second_order_associate.main_category,
    ocean_blue_cb_second_order_associate.customer_id,
    ocean_blue_cb_second_order_associate.order_date,
    ocean_blue_cb_second_order_associate.country_fullname,
    ocean_blue_cb_second_order_associate.subcategory_1,
    ocean_blue_cb_second_order_associate.order_count
   FROM ocean_blue_cb_second_order_associate
UNION
 SELECT electric_orange_cb_second_order_associate.main_category,
    electric_orange_cb_second_order_associate.customer_id,
    electric_orange_cb_second_order_associate.order_date,
    electric_orange_cb_second_order_associate.country_fullname,
    electric_orange_cb_second_order_associate.subcategory_1,
    electric_orange_cb_second_order_associate.order_count
   FROM electric_orange_cb_second_order_associate