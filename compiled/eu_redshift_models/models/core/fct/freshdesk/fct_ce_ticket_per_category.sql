--created by: Nham Dao
 


WITH cte_shopify AS (
        SELECT o.created_at::date AS order_date,
            --to_char(o.created_at::date::timestamp with time zone, 'Mon') AS month,
            date_trunc('month', o.created_at::date::timestamp with time zone) AS month,
            concat('Q', date_part('quarter', o.created_at::date)) AS quarter,
            count(DISTINCT o.order_number) AS orders
           FROM shopify_global.fct_order_enriched o
          GROUP BY (o.created_at::date), (to_char(o.created_at::date::timestamp with time zone, 'Mon'::text)), concat('Q', date_part('quarter', o.created_at::date))
        ), post_categorization AS (
         SELECT count(DISTINCT f.id) AS ticket_count,
            f.created_at::date AS creation_date,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Delivery'::text THEN 1
                    ELSE 0
                END) AS delivery,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text THEN 1
                    ELSE 0
                END) AS product_issues,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text THEN 1
                    ELSE 0
                END) AS main_catg_orders,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text THEN 1
                    ELSE 0
                END) AS suggestions_for_improvement,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'General Information'::text THEN 1
                    ELSE 0
                END) AS general_information,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Return'::text THEN 1
                    ELSE 0
                END) AS main_catg_return,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Questions'::text THEN 1
                    ELSE 0
                END) AS product_questions,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Internal Messages'::text THEN 1
                    ELSE 0
                END) AS internal_messages,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feeddback'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feedback'::text THEN 1
                    ELSE 0
                END) AS positive_feedback,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Delivery'::text AND f.custom_cf_subcategory_1::text = 'Delivery delay'::text THEN 1
                    ELSE 0
                END) AS subcat1_delivery_deliverydelay,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Delivery'::text AND f.custom_cf_subcategory_1::text = 'Wrong items received'::text THEN 1
                    ELSE 0
                END) AS subcat1_delivery_wrongitemsreceived,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Delivery'::text AND f.custom_cf_subcategory_1::text = 'Delivery status'::text THEN 1
                    ELSE 0
                END) AS subcat1_delivery_delivery_status,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Delivery'::text AND f.custom_cf_subcategory_1::text = 'Delivery not received'::text THEN 1
                    ELSE 0
                END) AS subcat1_delivery_deliverynotreceived,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'Issue / Defect'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_issuedefect,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'Bottle leaking'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_bottleleaking,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'No taste'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_notaste,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'User Issue'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_userissue,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'Cleaning'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_cleaning,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'Health constraints'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_healthconstraints,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = '"Air"'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_air,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'Mouthpiece taste'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_mouthpiecetaste,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'New issues / Trends'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_newissuestrends,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Issues'::text AND f.custom_cf_subcategory_1::text = 'Single Item lost'::text THEN 1
                    ELSE 0
                END) AS subcat1_productissues_singleitemlost,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Address change'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_address_change,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Invoice questions'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_invoicequestions,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Cancelation'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Cancellation'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_cancelation,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Payment questions'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_paymentquestions,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Order issues'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_orderissues,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Others'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_others,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Voucher code issues'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_vouchercodeissues,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Order'::text AND f.custom_cf_subcategory_1::text = 'Order change'::text THEN 1
                    ELSE 0
                END) AS subcat1_order_orderchange,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Return'::text AND f.custom_cf_subcategory_1::text = 'Return'::text THEN 1
                    ELSE 0
                END) AS subcat1_return,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'General Information'::text AND f.custom_cf_subcategory_1::text = 'Online Shop'::text THEN 1
                    ELSE 0
                END) AS subcat1_ginfo_onlineshop,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'General Information'::text AND f.custom_cf_subcategory_1::text = 'Cooperation request'::text THEN 1
                    ELSE 0
                END) AS subcat1_ginfo_cooperationrequest,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'General Information'::text AND f.custom_cf_subcategory_1::text = 'Criticism'::text THEN 1
                    ELSE 0
                END) AS subcat1_ginfo_criticism,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'General Information'::text AND f.custom_cf_subcategory_1::text = 'Merchant'::text THEN 1
                    ELSE 0
                END) AS subcat1_ginfo_merchant,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'General Information'::text AND f.custom_cf_subcategory_1::text = 'Others'::text THEN 1
                    ELSE 0
                END) AS subcat1_ginfo_others,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feedback'::text AND f.custom_cf_subcategory_1::text = 'Website'::text THEN 1
                    ELSE 0
                END) AS subcat1_posfeedback_website,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feedback'::text AND f.custom_cf_subcategory_1::text = 'General'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feeddback'::text AND f.custom_cf_subcategory_1::text = 'General'::text THEN 1
                    ELSE 0
                END) AS subcat1_posfeedback_general,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feedback'::text AND f.custom_cf_subcategory_1::text = 'Marketing Communication'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Positive Feeddback'::text AND f.custom_cf_subcategory_1::text = 'Marketing Communication'::text THEN 1
                    ELSE 0
                END) AS subcat1_posfeedback_marketingcommunication,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Questions'::text AND f.custom_cf_subcategory_1::text = 'Starter-Set'::text THEN 1
                    ELSE 0
                END) AS subcat1_prodquestions_starterset,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Questions'::text AND f.custom_cf_subcategory_1::text = 'Pods'::text THEN 1
                    ELSE 0
                END) AS subcat1_prodquestions_pods,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Questions'::text AND f.custom_cf_subcategory_1::text = 'Bottle'::text THEN 1
                    ELSE 0
                END) AS subcat1_prodquestions_bottle,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Product Questions'::text AND f.custom_cf_subcategory_1::text = 'Other Product Questions'::text THEN 1
                    ELSE 0
                END) AS subcat1_prodquestions_otherproductquestions,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Individualization'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_individualization,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Bottle size'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_bottlesize,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Bottle color'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_bottlecolor,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Taste properties'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_tasteproperties,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Individual'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_individual,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Material'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_material,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'New tastes'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_newtastes,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'Pods & Packaging'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_podspackaging,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'New idea'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_newidea,
            sum(
                CASE
                    WHEN f.custom_cf_category::text = 'Suggestions for improvement'::text AND f.custom_cf_subcategory_1::text = 'New Flavours'::text THEN 1
                    ELSE 0
                END) AS subcat1_sgi_newflavours,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery delay'::text AND f.custom_cf_subcategory_2::text = 'Problem DHL'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery not received'::text AND f.custom_cf_subcategory_2::text = 'Problem DHL'::text THEN 1
                    ELSE 0
                END) AS subcat2_deliverynotreceived_problemdhl,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery delay'::text AND f.custom_cf_subcategory_2::text = 'Problem Odoo / ERP'::text THEN 1
                    ELSE 0
                END) AS subcat2_deliverydelay_problemodooerp,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery delay'::text AND f.custom_cf_subcategory_2::text = 'Problem Geodis'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery not received'::text AND f.custom_cf_subcategory_2::text = 'Problem Geodis'::text THEN 1
                    ELSE 0
                END) AS subcat2_deliverydelay_problemgeodis,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery delay'::text AND f.custom_cf_subcategory_2::text = 'Customer mistake'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery lost'::text AND f.custom_cf_subcategory_2::text = 'Customer mistake'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery not received'::text AND f.custom_cf_subcategory_2::text = 'Customer mistake'::text THEN 1
                    ELSE 0
                END) AS subcat2_deliverydelay_customermistake,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery delay'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Wrong items received'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Return'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Address change'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Invoice questions'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery delay'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'No taste'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Others'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Order issues'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Order change'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery status'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Voucher code issues'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Individual'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'New tastes'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = '"Air"'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Bottle leaking'::text AND f.custom_cf_subcategory_2 IS NULL THEN 1
                    ELSE 0
                END) AS subcat2_none,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Online Shop'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Return'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Cancelation'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Cooperation request'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Bottle'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Pods'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'User Issue'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Cleaning'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery not received'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Criticism'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery not received'::text AND f.custom_cf_subcategory_2::text = 'Others'::text THEN 1
                    ELSE 0
                END) AS subcat2_others,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Online Shop'::text AND f.custom_cf_subcategory_2::text = 'Payment Methods'::text THEN 1
                    ELSE 0
                END) AS subcat2_onlineshop_paymentmethods,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Wrong items received'::text AND f.custom_cf_subcategory_2::text = 'Order incomplete'::text THEN 1
                    ELSE 0
                END) AS subcat2_wrong_items_received_orderincomplete,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Wrong items received'::text AND f.custom_cf_subcategory_2::text = 'Wrong products'::text THEN 1
                    ELSE 0
                END) AS subcat2_wrong_items_received_wrongproducts,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Delivery not received'::text AND f.custom_cf_subcategory_2::text = 'Letter lost'::text THEN 1
                    ELSE 0
                END) AS subcat2_deliverynotreceived_letterlost,
            sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'No taste'::text AND f.custom_cf_subcategory_2::text = 'N/A'::text THEN 1
                    ELSE 0
                END) + sum(
                CASE
                    WHEN f.custom_cf_subcategory_1::text = 'Bottle leaking'::text AND f.custom_cf_subcategory_2::text = 'N/A'::text THEN 1
                    ELSE 0
                END) AS subcat2_notaste_na
           FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" f
            WHERE date(f.created_at) > '2020-09-30'::date 
          -------##############----filter conditions-----######################------
         and spam is false    ----- Set to true if the ticket has been marked as spam
         and deleted is null ----- Set to true if the ticket has been deleted/trashed   
          GROUP BY (f.created_at::date)
          ----ORDER BY (f.created_at::date)
        ), unified_data AS (
         SELECT cte_shopify.order_date,
            cte_shopify.quarter,
            cte_shopify.month,
            cte_shopify.orders,
            post_categorization.ticket_count AS tickets,
            post_categorization.delivery,
            post_categorization.product_issues,
            post_categorization.main_catg_orders,
            post_categorization.suggestions_for_improvement,
            post_categorization.general_information,
            post_categorization.main_catg_return,
            post_categorization.product_questions,
            post_categorization.internal_messages,
            post_categorization.positive_feedback,
            post_categorization.subcat1_delivery_deliverydelay,
            post_categorization.subcat1_delivery_wrongitemsreceived,
            post_categorization.subcat1_return,
            post_categorization.subcat1_delivery_deliverynotreceived,
            post_categorization.subcat1_delivery_delivery_status,
            post_categorization.subcat1_ginfo_onlineshop,
            post_categorization.subcat1_ginfo_cooperationrequest,
            post_categorization.subcat1_ginfo_criticism,
            post_categorization.subcat1_ginfo_merchant,
            post_categorization.subcat1_ginfo_others,
            post_categorization.subcat1_posfeedback_website,
            post_categorization.subcat1_posfeedback_general,
            post_categorization.subcat1_posfeedback_marketingcommunication,
            post_categorization.subcat1_prodquestions_starterset,
            post_categorization.subcat1_prodquestions_pods,
            post_categorization.subcat1_prodquestions_bottle,
            post_categorization.subcat1_prodquestions_otherproductquestions,
            post_categorization.subcat1_order_address_change,
            post_categorization.subcat1_order_cancelation,
            post_categorization.subcat1_order_invoicequestions,
            post_categorization.subcat1_order_paymentquestions,
            post_categorization.subcat1_order_orderissues,
            post_categorization.subcat1_order_others,
            post_categorization.subcat1_order_vouchercodeissues,
            post_categorization.subcat1_order_orderchange,
            post_categorization.subcat1_productissues_issuedefect,
            post_categorization.subcat1_productissues_notaste,
            post_categorization.subcat1_productissues_bottleleaking,
            post_categorization.subcat1_productissues_userissue,
            post_categorization.subcat1_productissues_cleaning,
            post_categorization.subcat1_productissues_healthconstraints,
            post_categorization.subcat1_productissues_air,
            post_categorization.subcat1_productissues_mouthpiecetaste,
            post_categorization.subcat1_productissues_newissuestrends,
            post_categorization.subcat1_productissues_singleitemlost,
            post_categorization.subcat1_sgi_individualization,
            post_categorization.subcat1_sgi_bottlesize,
            post_categorization.subcat1_sgi_bottlecolor,
            post_categorization.subcat1_sgi_tasteproperties,
            post_categorization.subcat1_sgi_individual,
            post_categorization.subcat1_sgi_material,
            post_categorization.subcat1_sgi_newtastes,
            post_categorization.subcat1_sgi_podspackaging,
            post_categorization.subcat1_sgi_newidea,
            post_categorization.subcat1_sgi_newflavours,
            post_categorization.subcat2_deliverydelay_problemgeodis,
            post_categorization.subcat2_deliverynotreceived_problemdhl,
            post_categorization.subcat2_deliverydelay_problemodooerp,
            post_categorization.subcat2_deliverydelay_customermistake,
            post_categorization.subcat2_wrong_items_received_orderincomplete,
            post_categorization.subcat2_wrong_items_received_wrongproducts,
            post_categorization.subcat2_deliverynotreceived_letterlost,
            post_categorization.subcat2_notaste_na,
            post_categorization.subcat2_none,
            post_categorization.subcat2_others,
            post_categorization.subcat2_onlineshop_paymentmethods,
            sum(post_categorization.delivery) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS delivery_total,
            sum(post_categorization.product_issues) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS product_issues_total,
            sum(post_categorization.main_catg_orders) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS main_catg_orders_total,
            sum(post_categorization.suggestions_for_improvement) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS suggestions_for_improvement_total,
            sum(post_categorization.general_information) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS general_information_total,
            sum(post_categorization.main_catg_return) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS main_catg_return_total,
            sum(post_categorization.product_questions) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS product_questions_total,
            sum(post_categorization.internal_messages) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS internal_messages_total,
            sum(post_categorization.positive_feedback) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS positive_feedback_total,
            sum(post_categorization.subcat1_delivery_deliverydelay) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_delivery_deliverydelay_total,
            sum(post_categorization.subcat1_delivery_wrongitemsreceived) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_delivery_wrongitemsreceived_total,
            sum(post_categorization.subcat1_return) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_return_total,
            sum(post_categorization.subcat1_delivery_deliverynotreceived) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_delivery_deliverynotreceived_total,
            sum(post_categorization.subcat1_delivery_delivery_status) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_delivery_delivery_status_total,
            sum(post_categorization.subcat1_order_address_change) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_address_change_total,
            sum(post_categorization.subcat1_order_cancelation) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_cancelation_total,
            sum(post_categorization.subcat1_order_invoicequestions) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_invoicequestions_total,
            sum(post_categorization.subcat1_order_paymentquestions) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_paymentquestions_total,
            sum(post_categorization.subcat1_order_orderissues) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_orderissues_total,
            sum(post_categorization.subcat1_order_others) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_others_total,
            sum(post_categorization.subcat1_order_vouchercodeissues) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_vouchercodeissues_total,
            sum(post_categorization.subcat1_order_orderchange) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_order_orderchange_total,
            sum(post_categorization.subcat1_productissues_bottleleaking) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_bottleleaking_total,
            sum(post_categorization.subcat1_productissues_issuedefect) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_issuedefect_total,
            sum(post_categorization.subcat1_productissues_notaste) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_notaste_total,
            sum(post_categorization.subcat1_productissues_userissue) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_userissue_total,
            sum(post_categorization.subcat1_productissues_cleaning) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_cleaning_total,
            sum(post_categorization.subcat1_productissues_healthconstraints) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_healthconstraints_total,
            sum(post_categorization.subcat1_productissues_air) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_air_total,
            sum(post_categorization.subcat1_productissues_mouthpiecetaste) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_mouthpiecetaste_total,
            sum(post_categorization.subcat1_productissues_newissuestrends) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_newissuestrends_total,
            sum(post_categorization.subcat1_productissues_singleitemlost) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_productissues_singleitemlost_total,
            sum(post_categorization.subcat1_ginfo_onlineshop) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_ginfo_onlineshop_total,
            sum(post_categorization.subcat1_ginfo_cooperationrequest) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS subcat1_ginfo_cooperationrequest_total,
            sum(post_categorization.subcat1_ginfo_criticism) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_ginfo_criticism_total,
            sum(post_categorization.subcat1_ginfo_merchant) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_ginfo_merchant_total,
            sum(post_categorization.subcat1_ginfo_others) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_ginfo_others_total,
            sum(post_categorization.subcat1_posfeedback_website) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_posfeedback_website_total,
            sum(post_categorization.subcat1_posfeedback_general) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_posfeedback_general_total,
            sum(post_categorization.subcat1_posfeedback_marketingcommunication) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_posfeedback_mktngcomms_total,
            sum(post_categorization.subcat1_prodquestions_starterset) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_prodquestions_starterset_total,
            sum(post_categorization.subcat1_prodquestions_pods) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_prodquestions_pods_total,
            sum(post_categorization.subcat1_prodquestions_bottle) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_prodquestions_bottle_total,
            sum(post_categorization.subcat1_prodquestions_otherproductquestions) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_prodquestions_otherproductquestions_total,
            sum(post_categorization.subcat1_sgi_individualization) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_individualization_total,
            sum(post_categorization.subcat1_sgi_bottlesize) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_bottlesize_total,
            sum(post_categorization.subcat1_sgi_bottlecolor) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_bottlecolor_total,
            sum(post_categorization.subcat1_sgi_tasteproperties) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_tasteproperties_total,
            sum(post_categorization.subcat1_sgi_individual) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_individual_total,
            sum(post_categorization.subcat1_sgi_material) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_material_total,
            sum(post_categorization.subcat1_sgi_newtastes) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_newtastes_total,
            sum(post_categorization.subcat1_sgi_podspackaging) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_podspackaging_total,
            sum(post_categorization.subcat1_sgi_newidea) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_newidea_total,
            sum(post_categorization.subcat1_sgi_newflavours) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat1_sgi_newflavours_total,
            sum(post_categorization.subcat2_deliverydelay_problemgeodis) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_deliverydelay_problemgeodis_total,
            sum(post_categorization.subcat2_deliverynotreceived_problemdhl) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_deliverynotreceived_problemdhl_total,
            sum(post_categorization.subcat2_deliverydelay_customermistake) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_deliverydelay_customermistake_total,
            sum(post_categorization.subcat2_wrong_items_received_orderincomplete) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_wrong_items_received_orderincomplete_total,
            sum(post_categorization.subcat2_wrong_items_received_wrongproducts) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_wrong_items_received_wrongproducts_total,
            sum(post_categorization.subcat2_deliverynotreceived_letterlost) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_deliverynotreceived_letterlost_total,
            sum(post_categorization.subcat2_deliverydelay_problemodooerp) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_deliverydelay_problemodooerp_total,
            sum(post_categorization.subcat2_notaste_na) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_notaste_na_total,
            sum(post_categorization.subcat2_none) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_none_total,
            sum(post_categorization.subcat2_others) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_others_total,
            sum(post_categorization.subcat2_onlineshop_paymentmethods) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS subcat2_onlineshop_paymentmethods_total,
            sum(cte_shopify.orders) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS order_total,
            sum(post_categorization.ticket_count) OVER (PARTITION BY cte_shopify.month ORDER BY cte_shopify.order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ticket_total
           FROM cte_shopify
             JOIN post_categorization ON post_categorization.creation_date = cte_shopify.order_date
          GROUP BY cte_shopify.order_date, cte_shopify.quarter, cte_shopify.month, cte_shopify.orders, post_categorization.ticket_count, post_categorization.delivery, post_categorization.product_issues, post_categorization.main_catg_orders, post_categorization.suggestions_for_improvement, post_categorization.general_information, post_categorization.main_catg_return, post_categorization.product_questions, post_categorization.internal_messages, post_categorization.positive_feedback, post_categorization.subcat1_delivery_deliverydelay, post_categorization.subcat1_delivery_wrongitemsreceived, post_categorization.subcat1_return, post_categorization.subcat1_delivery_deliverynotreceived, post_categorization.subcat1_delivery_delivery_status, post_categorization.subcat1_ginfo_onlineshop, post_categorization.subcat1_ginfo_cooperationrequest, post_categorization.subcat1_ginfo_criticism, post_categorization.subcat1_ginfo_merchant, post_categorization.subcat1_ginfo_others, post_categorization.subcat1_posfeedback_website, post_categorization.subcat1_posfeedback_general, post_categorization.subcat1_posfeedback_marketingcommunication, post_categorization.subcat1_prodquestions_starterset, post_categorization.subcat1_prodquestions_pods, post_categorization.subcat1_prodquestions_bottle, post_categorization.subcat1_prodquestions_otherproductquestions, post_categorization.subcat1_order_address_change, post_categorization.subcat1_order_cancelation, post_categorization.subcat1_order_invoicequestions, post_categorization.subcat1_order_paymentquestions, post_categorization.subcat1_order_orderissues, post_categorization.subcat1_order_others, post_categorization.subcat1_order_vouchercodeissues, post_categorization.subcat1_order_orderchange, post_categorization.subcat1_productissues_issuedefect, post_categorization.subcat1_productissues_notaste, post_categorization.subcat1_productissues_bottleleaking, post_categorization.subcat1_productissues_userissue, post_categorization.subcat1_productissues_cleaning, post_categorization.subcat1_productissues_healthconstraints, post_categorization.subcat1_productissues_air, post_categorization.subcat1_productissues_mouthpiecetaste, post_categorization.subcat1_productissues_newissuestrends, post_categorization.subcat1_productissues_singleitemlost, post_categorization.subcat1_sgi_individualization, post_categorization.subcat1_sgi_bottlesize, post_categorization.subcat1_sgi_bottlecolor, post_categorization.subcat1_sgi_tasteproperties, post_categorization.subcat1_sgi_individual, post_categorization.subcat1_sgi_material, post_categorization.subcat1_sgi_newtastes, post_categorization.subcat1_sgi_podspackaging, post_categorization.subcat1_sgi_newidea, post_categorization.subcat1_sgi_newflavours, post_categorization.subcat2_deliverydelay_problemgeodis, post_categorization.subcat2_deliverynotreceived_problemdhl, post_categorization.subcat2_deliverydelay_problemodooerp, post_categorization.subcat2_deliverydelay_customermistake, post_categorization.subcat2_wrong_items_received_orderincomplete, post_categorization.subcat2_wrong_items_received_wrongproducts, post_categorization.subcat2_deliverynotreceived_letterlost, post_categorization.subcat2_notaste_na, post_categorization.subcat2_none, post_categorization.subcat2_others, post_categorization.subcat2_onlineshop_paymentmethods
          ORDER BY cte_shopify.order_date
        )
 SELECT unified_data.order_date,
    unified_data.quarter,
    unified_data.month,
    unified_data.orders,
    unified_data.tickets,
    unified_data.delivery,
    unified_data.product_issues,
    unified_data.main_catg_orders,
    unified_data.suggestions_for_improvement,
    unified_data.general_information,
    unified_data.main_catg_return,
    unified_data.product_questions,
    unified_data.internal_messages,
    unified_data.positive_feedback,
    unified_data.subcat1_delivery_deliverydelay,
    unified_data.subcat1_delivery_wrongitemsreceived,
    unified_data.subcat1_return,
    unified_data.subcat1_delivery_deliverynotreceived,
    unified_data.subcat1_delivery_delivery_status,
    unified_data.subcat1_ginfo_onlineshop,
    unified_data.subcat1_ginfo_cooperationrequest,
    unified_data.subcat1_ginfo_criticism,
    unified_data.subcat1_ginfo_merchant,
    unified_data.subcat1_ginfo_others,
    unified_data.subcat1_posfeedback_website,
    unified_data.subcat1_posfeedback_general,
    unified_data.subcat1_posfeedback_marketingcommunication,
    unified_data.subcat1_prodquestions_starterset,
    unified_data.subcat1_prodquestions_pods,
    unified_data.subcat1_prodquestions_bottle,
    unified_data.subcat1_prodquestions_otherproductquestions,
    unified_data.subcat1_order_address_change,
    unified_data.subcat1_order_cancelation,
    unified_data.subcat1_order_invoicequestions,
    unified_data.subcat1_order_paymentquestions,
    unified_data.subcat1_order_orderissues,
    unified_data.subcat1_order_others,
    unified_data.subcat1_order_vouchercodeissues,
    unified_data.subcat1_order_orderchange,
    unified_data.subcat1_productissues_issuedefect,
    unified_data.subcat1_productissues_notaste,
    unified_data.subcat1_productissues_bottleleaking,
    unified_data.subcat1_productissues_userissue,
    unified_data.subcat1_productissues_cleaning,
    unified_data.subcat1_productissues_healthconstraints,
    unified_data.subcat1_productissues_air,
    unified_data.subcat1_productissues_mouthpiecetaste,
    unified_data.subcat1_productissues_newissuestrends,
    unified_data.subcat1_productissues_singleitemlost,
    unified_data.subcat1_sgi_individualization,
    unified_data.subcat1_sgi_bottlesize,
    unified_data.subcat1_sgi_bottlecolor,
    unified_data.subcat1_sgi_tasteproperties,
    unified_data.subcat1_sgi_individual,
    unified_data.subcat1_sgi_material,
    unified_data.subcat1_sgi_newtastes,
    unified_data.subcat1_sgi_podspackaging,
    unified_data.subcat1_sgi_newidea,
    unified_data.subcat1_sgi_newflavours,
    unified_data.subcat2_deliverydelay_problemgeodis,
    unified_data.subcat2_deliverynotreceived_problemdhl,
    unified_data.subcat2_deliverydelay_problemodooerp,
    unified_data.subcat2_deliverydelay_customermistake,
    unified_data.subcat2_wrong_items_received_orderincomplete,
    unified_data.subcat2_wrong_items_received_wrongproducts,
    unified_data.subcat2_deliverynotreceived_letterlost,
    unified_data.subcat2_notaste_na,
    unified_data.subcat2_none,
    unified_data.subcat2_others,
    unified_data.subcat2_onlineshop_paymentmethods
   FROM unified_data