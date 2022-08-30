

WITH influencer_voucher_no_campaigns AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country = (country_system_account_mapping.country_fullname)
          WHERE (dim_influencer_enriched_1.coupon_code IS NOT NULL OR dim_influencer_enriched_1.coupon_code <> 'undefined') AND (dim_influencer_enriched_1.campaign = 'undefined' OR dim_influencer_enriched_1.campaign IS NULL)
        ), influencer_campaigns_no_voucher AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country = (country_system_account_mapping.country_fullname)
          WHERE dim_influencer_enriched_1.campaign IS NOT NULL AND dim_influencer_enriched_1.campaign <> 'undefined' AND (dim_influencer_enriched_1.coupon_code = 'undefined' OR dim_influencer_enriched_1.coupon_code IS NULL)
        ), influencer_voucher_campaigns AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country = (country_system_account_mapping.country_fullname)
          WHERE dim_influencer_enriched_1.campaign IS NOT NULL AND dim_influencer_enriched_1.campaign <> 'undefined' AND dim_influencer_enriched_1.coupon_code <> 'undefined' AND dim_influencer_enriched_1.coupon_code IS NOT NULL
        ), influencer_voucher_campaigns_only_voucher AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code
--            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country = (country_system_account_mapping.country_fullname)
          WHERE dim_influencer_enriched_1.campaign IS NOT NULL AND dim_influencer_enriched_1.campaign <> 'undefined' AND dim_influencer_enriched_1.coupon_code <> 'undefined' AND dim_influencer_enriched_1.coupon_code IS NOT NULL
        ), sessions_aggregated AS (
         SELECT sessions_union.utm_campaign_name,
            sessions_union.day AS created_at,
            sum(sessions_union.users) AS users,
            sum(sessions_union.sessions) AS sessions
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_sessions_union" sessions_union
          GROUP BY sessions_union.utm_campaign_name, sessions_union.day
        ), new_user AS (
         SELECT customer_reorder_count.customer_id,
            -- customer_reorder_count.customer_name,
            customer_reorder_count.customer_email,
            'New Customer'::text AS customer_flag
           FROM ( SELECT order_enriched.customer_id,
                    -- rtrim(order_enriched.shipping_address_name::text, ' '::text) AS customer_name,
                    order_enriched.email AS customer_email,
                    count(order_enriched.customer_id) AS reorder_count
                   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched
                  WHERE order_enriched.financial_status in ('paid', 'partially_refunded')
                  GROUP BY order_enriched.customer_id, order_enriched.email) customer_reorder_count
          WHERE customer_reorder_count.reorder_count = 1
        ), revenue_campaigns_voucher AS (
         SELECT DISTINCT sales_union.customer_id,
            influencer_voucher_campaigns.region,
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END AS new_users,
            sales_union.utm_campaign_name,
            discounts_union.discount_code,
            influencer_voucher_campaigns.coupon_code,
            order_enriched.created_at::date AS created_at,
            sum(COALESCE(sales_union.orders, 0)) AS orders,
            sum(COALESCE(discounts_union.total_discount_amount, 0::double precision)) AS total_discount_amount,
            sum(
                CASE
                    WHEN upper(influencer_voucher_campaigns.coupon_code) = upper(COALESCE(discounts_union.discount_code, ' ')) AND (COALESCE(discounts_union.total_discount_amount, 0::double precision) < 0::double precision OR discounts_union.discount_type = 'Free shipping'::text) THEN COALESCE(order_enriched.total_price, 0::double precision)
                    ELSE 0::double precision
                END) AS no_campaign_transaction_revenue,
            sum(
                CASE
                    WHEN upper(influencer_voucher_campaigns.coupon_code) = upper(COALESCE(discounts_union.discount_code, ' ')) AND (COALESCE(discounts_union.total_discount_amount, 0::double precision) < 0::double precision OR discounts_union.discount_type = 'Free shipping'::text) THEN
                    CASE
                        WHEN order_enriched.net_revenue_2 IS NULL AND order_enriched.total_price > 0::double precision THEN order_enriched.total_price - COALESCE(discounts_union.total_tax_amount, 0::double precision) + COALESCE(discounts_union.total_discount_amount, 0::double precision)
                        ELSE COALESCE(order_enriched.net_revenue_2, 0::double precision)
                    END
                    ELSE 0::double precision
                END) AS no_campaign_net_revenue,
            sum(
                CASE
                    WHEN upper(influencer_voucher_campaigns.coupon_code) <> upper(COALESCE(discounts_union.discount_code, ' ')) THEN COALESCE(order_enriched.total_price, 0::double precision)
                    ELSE 0::double precision
                END) AS campaign_transaction_revenue,
            sum(
                CASE
                    WHEN upper(influencer_voucher_campaigns.coupon_code) <> upper(COALESCE(discounts_union.discount_code, ' ')) THEN
                    CASE
                        WHEN order_enriched.net_revenue_2 IS NULL AND order_enriched.total_price > 0::double precision THEN order_enriched.total_price - COALESCE(discounts_union.total_tax_amount, 0::double precision) + COALESCE(discounts_union.total_discount_amount, 0::double precision)
                        ELSE COALESCE(order_enriched.net_revenue_2, 0::double precision)
                    END
                    ELSE 0::double precision
                END) AS campaign_transaction_net_revenue
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_sales_union" sales_union
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched ON sales_union.customer_id = order_enriched.customer_id AND sales_union.created_at = date(order_enriched.created_at)
             LEFT JOIN "airup_eu_dwh"."shopify_influencer_analytics"."fct_discounts_union" discounts_union ON sales_union.customer_id = discounts_union.customer_id
             LEFT JOIN influencer_voucher_campaigns ON sales_union.utm_campaign_name = influencer_voucher_campaigns.campaign
             LEFT JOIN new_user ON sales_union.customer_id = new_user.customer_id
          WHERE sales_union.utm_campaign_name = influencer_voucher_campaigns.campaign AND order_enriched.created_at::date IS NOT NULL
          GROUP BY sales_union.customer_id, influencer_voucher_campaigns.region, (
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END), sales_union.utm_campaign_name, discounts_union.discount_code, influencer_voucher_campaigns.coupon_code, (order_enriched.created_at::date)
        ), revenue_voucher_no_campaigns AS (
         SELECT DISTINCT discounts_union.customer_id,
            discounts_union.customer_name,
            discounts_union.customer_email,
            influencer_voucher_no_campaigns.region,
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END AS new_users,
            'undefined'::text AS utm_campaign_name,
            influencer_voucher_no_campaigns.coupon_code,
            discounts_union.discount_code,
            order_enriched.created_at::date AS created_at,
            sum(COALESCE(discounts_union.orders, 0)) AS orders,
            sum(COALESCE(order_enriched.total_price, 0::double precision)) AS no_campaign_transaction_revenue,
            sum(
                CASE
                    WHEN order_enriched.net_revenue_2 IS NULL AND order_enriched.total_price > 0::double precision THEN order_enriched.total_price - COALESCE(discounts_union.total_tax_amount, 0::double precision) + COALESCE(discounts_union.total_discount_amount, 0::double precision)
                    ELSE COALESCE(order_enriched.net_revenue_2, 0::double precision)
                END) AS no_campaign_net_revenue,
            sum(COALESCE(discounts_union.total_discount_amount, 0::double precision)) AS total_discount_amount
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_discounts_union" discounts_union
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched ON discounts_union.customer_id = order_enriched.customer_id AND discounts_union.created_at = date(order_enriched.created_at)
             JOIN influencer_voucher_no_campaigns ON discounts_union.discount_code = influencer_voucher_no_campaigns.coupon_code
             LEFT JOIN new_user ON discounts_union.customer_id = new_user.customer_id
          WHERE order_enriched.created_at::date IS NOT NULL
          GROUP BY discounts_union.customer_id, discounts_union.customer_name, discounts_union.customer_email, influencer_voucher_no_campaigns.region, (
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END), influencer_voucher_no_campaigns.coupon_code, discounts_union.discount_code, (order_enriched.created_at::date)
        ), revenue_campaigns_no_voucher AS (
         SELECT DISTINCT sales_union.customer_id,
            sales_union.customer_name,
            sales_union.customer_email,
            influencer_campaigns_no_voucher.region,
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END AS new_users,
            sales_union.utm_campaign_name,
            influencer_campaigns_no_voucher.coupon_code,
            -- NULL::text AS discount_code,
            order_enriched.created_at::date AS created_at,
            sum(COALESCE(sales_union.orders, 0)) AS orders,
            0::double precision AS no_campaign_transaction_revenue,
            0::double precision AS no_campaign_net_revenue,
            sum(COALESCE(order_enriched.total_price, 0::double precision)) AS campaign_transaction_revenue,
            sum(
                CASE
                    WHEN order_enriched.net_revenue_2 IS NULL AND order_enriched.total_price > 0::double precision THEN order_enriched.total_price - COALESCE(sales_union.taxes, 0::double precision) + COALESCE(sales_union.discounts, 0::double precision)
                    ELSE COALESCE(order_enriched.net_revenue_2, 0::double precision)
                END) AS campaign_transaction_net_revenue,
            0::double precision AS total_discount_amount
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_sales_union" sales_union
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched ON sales_union.customer_id = order_enriched.customer_id AND sales_union.created_at = date(order_enriched.created_at)
             LEFT JOIN influencer_campaigns_no_voucher ON sales_union.utm_campaign_name = influencer_campaigns_no_voucher.campaign
             LEFT JOIN new_user ON sales_union.customer_id = new_user.customer_id
          WHERE sales_union.utm_campaign_name = influencer_campaigns_no_voucher.campaign AND order_enriched.created_at::date IS NOT NULL
          GROUP BY sales_union.customer_id, sales_union.customer_name, sales_union.customer_email, influencer_campaigns_no_voucher.region, (
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END), sales_union.utm_campaign_name, influencer_campaigns_no_voucher.coupon_code, (order_enriched.created_at::date)
        ), campaign_data_aggregated AS (
         SELECT revenue_campaigns_voucher.region,
            revenue_campaigns_voucher.utm_campaign_name AS campaign,
            revenue_campaigns_voucher.coupon_code,
            revenue_campaigns_voucher.created_at,
            sum(revenue_campaigns_voucher.new_users) AS new_users,
            sum(revenue_campaigns_voucher.orders) AS orders,
            sum(revenue_campaigns_voucher.no_campaign_transaction_revenue) AS no_campaign_transaction_revenue,
            sum(revenue_campaigns_voucher.no_campaign_net_revenue) AS no_campaign_net_revenue,
            sum(revenue_campaigns_voucher.campaign_transaction_revenue) AS campaign_transaction_revenue,
            sum(revenue_campaigns_voucher.campaign_transaction_net_revenue) AS campaign_transaction_net_revenue
           FROM revenue_campaigns_voucher
          GROUP BY revenue_campaigns_voucher.region, revenue_campaigns_voucher.utm_campaign_name, revenue_campaigns_voucher.coupon_code, revenue_campaigns_voucher.created_at
        UNION ALL
         SELECT revenue_campaigns_no_voucher.region,
            revenue_campaigns_no_voucher.utm_campaign_name AS campaign,
            revenue_campaigns_no_voucher.coupon_code,
            revenue_campaigns_no_voucher.created_at,
            sum(revenue_campaigns_no_voucher.new_users) AS new_users,
            sum(revenue_campaigns_no_voucher.orders) AS orders,
            sum(revenue_campaigns_no_voucher.no_campaign_transaction_revenue) AS no_campaign_transaction_revenue,
            sum(revenue_campaigns_no_voucher.no_campaign_net_revenue) AS no_campaign_net_revenue,
            sum(revenue_campaigns_no_voucher.campaign_transaction_revenue) AS campaign_transaction_revenue,
            sum(revenue_campaigns_no_voucher.campaign_transaction_net_revenue) AS campaign_transaction_net_revenue
           FROM revenue_campaigns_no_voucher
          GROUP BY revenue_campaigns_no_voucher.region, revenue_campaigns_no_voucher.utm_campaign_name, revenue_campaigns_no_voucher.coupon_code, revenue_campaigns_no_voucher.created_at
        ), transaction_revenue_influencer_campaign_aggregated AS (
         SELECT DISTINCT campaign_data_aggregated.region,
            campaign_data_aggregated.campaign,
            campaign_data_aggregated.coupon_code,
            campaign_data_aggregated.created_at,
            sum(COALESCE(campaign_data_aggregated.new_users, 0::bigint)) AS new_users,
            sum(COALESCE(campaign_data_aggregated.orders, 0::numeric)) AS orders,
            sum(COALESCE(campaign_data_aggregated.no_campaign_transaction_revenue, 0::double precision)) AS no_campaign_transaction_revenue,
            sum(COALESCE(campaign_data_aggregated.no_campaign_net_revenue, 0::double precision)) AS no_campaign_net_revenue,
            sum(COALESCE(campaign_data_aggregated.campaign_transaction_revenue, 0::double precision)) AS campaign_transaction_revenue,
            sum(COALESCE(campaign_data_aggregated.campaign_transaction_net_revenue, 0::double precision)) AS campaign_transaction_net_revenue,
            0::double precision AS no_influencer_campaign_transaction_revenue,
            0::double precision AS no_influencer_campaign_net_revenue,
            sum(COALESCE(sessions_aggregated.users, 0::bigint)) / count(*) OVER (PARTITION BY campaign_data_aggregated.campaign, campaign_data_aggregated.created_at /* ORDER BY campaign_data_aggregated.campaign */)::numeric AS users,
            sum(COALESCE(sessions_aggregated.sessions, 0::bigint)) / count(*) OVER (PARTITION BY campaign_data_aggregated.campaign, campaign_data_aggregated.created_at /* ORDER BY campaign_data_aggregated.campaign */)::numeric AS sessions
           FROM campaign_data_aggregated
             LEFT JOIN sessions_aggregated ON campaign_data_aggregated.campaign = sessions_aggregated.utm_campaign_name AND campaign_data_aggregated.created_at = sessions_aggregated.created_at
          GROUP BY campaign_data_aggregated.region, campaign_data_aggregated.campaign, campaign_data_aggregated.coupon_code, campaign_data_aggregated.created_at
        ), transaction_revenue_no_campaign AS (
         SELECT revenue_voucher_no_campaigns.region,
            'undefined'::text AS campaign,
            revenue_voucher_no_campaigns.coupon_code,
            revenue_voucher_no_campaigns.created_at,
            sum(COALESCE(revenue_voucher_no_campaigns.new_users, 0)) AS new_users,
            sum(COALESCE(revenue_voucher_no_campaigns.orders, 0::bigint)) AS orders,
            sum(COALESCE(revenue_voucher_no_campaigns.no_campaign_transaction_revenue, 0::double precision)) AS no_campaign_transaction_revenue,
            sum(COALESCE(revenue_voucher_no_campaigns.no_campaign_net_revenue, 0::double precision)) AS no_campaign_net_revenue,
            0::double precision AS campaign_transaction_revenue,
            0::double precision AS campaign_transaction_net_revenue,
            0::double precision AS no_influencer_campaign_transaction_revenue,
            0::double precision AS no_influencer_campaign_net_revenue,
            0::double precision AS users,
            0::double precision AS sessions
           FROM revenue_voucher_no_campaigns
          GROUP BY revenue_voucher_no_campaigns.region, revenue_voucher_no_campaigns.coupon_code, revenue_voucher_no_campaigns.created_at
        ), sub_query_no_campaign_logic as (
        select discounts_union.customer_email, discounts_union.created_at
        FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_discounts_union" discounts_union
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched ON discounts_union.customer_id = order_enriched.customer_id AND discounts_union.created_at = date(order_enriched.created_at)
             LEFT JOIN influencer_voucher_campaigns ON discounts_union.discount_code = influencer_voucher_campaigns.coupon_code
             LEFT JOIN new_user ON discounts_union.customer_id = new_user.customer_id
        where  influencer_voucher_campaigns.coupon_code IS NOT null
        except
        SELECT 
                    discounts_union_1.customer_email,
                    discounts_union_1.created_at
        FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_sales_union" sales_union
        JOIN "airup_eu_dwh"."shopify_influencer_analytics"."fct_discounts_union" discounts_union_1 ON sales_union.customer_id = discounts_union_1.customer_id AND sales_union.created_at = discounts_union_1.created_at
        JOIN influencer_voucher_campaigns influencer_voucher_campaigns_1 ON sales_union.utm_campaign_name = influencer_voucher_campaigns_1.campaign
        ), influencer_no_campaign_logic AS (
         SELECT discounts_union.customer_id,
            -- discounts_union.customer_name,
            discounts_union.customer_email,
            influencer_voucher_campaigns_only_voucher.region,
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END AS new_users,
            'undefined'::text AS utm_campaign_name,
            influencer_voucher_campaigns_only_voucher.coupon_code,
            discounts_union.discount_code,
            discounts_union.created_at,
            sum(discounts_union.orders) AS orders,
            sum(
                CASE
                    WHEN order_enriched.created_at IS NULL THEN COALESCE(discounts_union.grand_total_sales, 0::double precision)
                    ELSE COALESCE(order_enriched.total_price, 0::double precision)
                END) AS no_influencer_campaign_transaction_revenue,
            sum(
                CASE
                    WHEN order_enriched.net_revenue_2 IS NULL AND order_enriched.total_price > 0::double precision THEN order_enriched.total_price - COALESCE(discounts_union.total_tax_amount, 0::double precision) + COALESCE(discounts_union.total_discount_amount, 0::double precision)
                    WHEN order_enriched.created_at IS NULL THEN discounts_union.grand_total_sales - COALESCE(discounts_union.total_tax_amount, 0::double precision) + COALESCE(discounts_union.total_discount_amount, 0::double precision)
                    ELSE COALESCE(order_enriched.net_revenue_2, 0::double precision)
                END) AS no_influencer_campaign_net_revenue,
            sum(COALESCE(discounts_union.total_discount_amount, 0::double precision)) AS total_discount_amount
           FROM "airup_eu_dwh"."shopify_influencer_analytics"."fct_discounts_union" discounts_union
             LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" order_enriched ON discounts_union.customer_id = order_enriched.customer_id AND discounts_union.created_at = date(order_enriched.created_at)
             LEFT JOIN influencer_voucher_campaigns_only_voucher ON discounts_union.discount_code = influencer_voucher_campaigns_only_voucher.coupon_code
             LEFT JOIN new_user ON discounts_union.customer_id = new_user.customer_id
          WHERE (md5(discounts_union.customer_email||discounts_union.created_at::text) IN (SELECT md5(customer_email||created_at::text) from sub_query_no_campaign_logic )) 
          AND influencer_voucher_campaigns_only_voucher.coupon_code IS NOT NULL
          GROUP BY discounts_union.customer_id, discounts_union.customer_email, influencer_voucher_campaigns_only_voucher.region, (
                CASE
                    WHEN new_user.customer_flag = 'New Customer'::text THEN 1
                    ELSE 0
                END), influencer_voucher_campaigns_only_voucher.coupon_code, discounts_union.discount_code, discounts_union.created_at
        ), transaction_revenue_no_influencer_campaign AS (
         SELECT influencer_no_campaign_logic.region,
            'undefined'::text AS campaign,
            influencer_no_campaign_logic.coupon_code,
            sum(influencer_no_campaign_logic.new_users) AS new_users,
            sum(influencer_no_campaign_logic.orders) AS orders,
            0::double precision AS no_campaign_transaction_revenue,
            0::double precision AS no_campaign_net_revenue,
            0::double precision AS campaign_transaction_revenue,
            0::double precision AS campaign_transaction_net_revenue,
            sum(influencer_no_campaign_logic.no_influencer_campaign_transaction_revenue) AS no_influencer_campaign_transaction_revenue,
            sum(influencer_no_campaign_logic.no_influencer_campaign_net_revenue) AS no_influencer_campaign_net_revenue,
            0::double precision AS users,
            0::double precision AS sessions
           FROM influencer_no_campaign_logic
          GROUP BY influencer_no_campaign_logic.region, influencer_no_campaign_logic.coupon_code
        ), all_transactions_union AS (
         SELECT transaction_revenue_no_campaign.region,
            transaction_revenue_no_campaign.campaign,
            transaction_revenue_no_campaign.coupon_code,
            transaction_revenue_no_campaign.created_at,
            transaction_revenue_no_campaign.new_users,
            transaction_revenue_no_campaign.orders,
            transaction_revenue_no_campaign.no_campaign_transaction_revenue,
            transaction_revenue_no_campaign.no_campaign_net_revenue,
            transaction_revenue_no_campaign.campaign_transaction_revenue,
            transaction_revenue_no_campaign.campaign_transaction_net_revenue,
            transaction_revenue_no_campaign.no_influencer_campaign_transaction_revenue,
            transaction_revenue_no_campaign.no_influencer_campaign_net_revenue,
            transaction_revenue_no_campaign.users,
            transaction_revenue_no_campaign.sessions
           FROM transaction_revenue_no_campaign
        UNION ALL
         SELECT transaction_revenue_influencer_campaign_aggregated.region,
            transaction_revenue_influencer_campaign_aggregated.campaign,
            transaction_revenue_influencer_campaign_aggregated.coupon_code,
            transaction_revenue_influencer_campaign_aggregated.created_at,
            transaction_revenue_influencer_campaign_aggregated.new_users,
            transaction_revenue_influencer_campaign_aggregated.orders,
            transaction_revenue_influencer_campaign_aggregated.no_campaign_transaction_revenue,
            transaction_revenue_influencer_campaign_aggregated.no_campaign_net_revenue,
            transaction_revenue_influencer_campaign_aggregated.campaign_transaction_revenue,
            transaction_revenue_influencer_campaign_aggregated.campaign_transaction_net_revenue,
            transaction_revenue_influencer_campaign_aggregated.no_influencer_campaign_transaction_revenue,
            transaction_revenue_influencer_campaign_aggregated.no_influencer_campaign_net_revenue,
            transaction_revenue_influencer_campaign_aggregated.users,
            transaction_revenue_influencer_campaign_aggregated.sessions
           FROM transaction_revenue_influencer_campaign_aggregated
        ), transactions_revenue_final_aggregation AS (
         SELECT all_transactions_union.region,
            all_transactions_union.campaign,
            all_transactions_union.coupon_code,
            sum(all_transactions_union.users) AS users,
            sum(all_transactions_union.new_users) AS new_users,
            sum(all_transactions_union.sessions) AS sessions,
            sum(all_transactions_union.orders) AS orders,
            sum(all_transactions_union.campaign_transaction_revenue) AS campaign_transaction_revenue,
            sum(all_transactions_union.campaign_transaction_net_revenue) AS utm_net_revenue,
            sum(all_transactions_union.no_campaign_net_revenue + all_transactions_union.no_influencer_campaign_net_revenue) AS voucher_net_revenue,
            sum(all_transactions_union.campaign_transaction_net_revenue + all_transactions_union.no_campaign_net_revenue + all_transactions_union.no_influencer_campaign_net_revenue) AS overall_net_revenue,
            sum(all_transactions_union.no_campaign_net_revenue) AS no_campaign_net_revenue,
            sum(all_transactions_union.no_influencer_campaign_transaction_revenue) AS no_influencer_campaign_transaction_revenue,
            sum(all_transactions_union.no_influencer_campaign_net_revenue) AS no_influencer_campaign_net_revenue
           FROM all_transactions_union
          GROUP BY all_transactions_union.region, all_transactions_union.campaign, all_transactions_union.coupon_code
        ), all_influencer_file_campaigns_coupons AS (
         SELECT DISTINCT dim_influencer_enriched_1.id,
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END AS coupon_code,
            transactions_revenue_final_aggregation.campaign,
            dim_influencer_enriched_1.date,
            dim_influencer_enriched_1.campaign_status,
            count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status) AS count_groups,
            sum(transactions_revenue_final_aggregation.users) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS users,
            sum(transactions_revenue_final_aggregation.new_users) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::numeric AS new_users,
            sum(transactions_revenue_final_aggregation.sessions) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS sessions,
            sum(transactions_revenue_final_aggregation.orders) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::numeric AS orders,
            sum(transactions_revenue_final_aggregation.campaign_transaction_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS campaign_transaction_revenue,
            sum(transactions_revenue_final_aggregation.utm_net_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS utm_net_revenue,
            sum(transactions_revenue_final_aggregation.voucher_net_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS voucher_net_revenue,
            sum(transactions_revenue_final_aggregation.overall_net_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS overall_net_revenue,
            sum(transactions_revenue_final_aggregation.no_campaign_net_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS no_campaign_net_revenue,
            sum(transactions_revenue_final_aggregation.no_influencer_campaign_transaction_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), dim_influencer_enriched_1.campaign_status)::double precision AS no_influencer_campaign_transaction_revenue,
            sum(transactions_revenue_final_aggregation.no_influencer_campaign_net_revenue) / count(*) OVER (PARTITION BY transactions_revenue_final_aggregation.campaign, (
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                END), dim_influencer_enriched_1.campaign_status)::double precision AS no_influencer_campaign_net_revenue
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched"  dim_influencer_enriched_1
             JOIN transactions_revenue_final_aggregation ON dim_influencer_enriched_1.campaign = transactions_revenue_final_aggregation.campaign AND
                CASE
                    WHEN dim_influencer_enriched_1.coupon_code IS NULL THEN 'undefined'
                    ELSE dim_influencer_enriched_1.coupon_code
                END =
                CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END
             LEFT JOIN transaction_revenue_no_influencer_campaign ON transaction_revenue_no_influencer_campaign.coupon_code = dim_influencer_enriched_1.coupon_code
          GROUP BY dim_influencer_enriched_1.id, 
                (CASE
                    WHEN transactions_revenue_final_aggregation.coupon_code IS NULL THEN 'undefined'
                    ELSE transactions_revenue_final_aggregation.coupon_code
                END), transactions_revenue_final_aggregation.coupon_code,transactions_revenue_final_aggregation.campaign, dim_influencer_enriched_1.date, dim_influencer_enriched_1.campaign_status
        UNION ALL
         SELECT DISTINCT dim_influencer_enriched_1.id,
            transaction_revenue_no_influencer_campaign.coupon_code,
            COALESCE(transaction_revenue_no_influencer_campaign.campaign, 'undefined') AS campaign,
            dim_influencer_enriched_1.date,
            dim_influencer_enriched_1.campaign_status,
            count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status) AS count_groups,
            sum(transaction_revenue_no_influencer_campaign.users) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS users,
            sum(transaction_revenue_no_influencer_campaign.new_users) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::numeric AS new_users,
            sum(transaction_revenue_no_influencer_campaign.sessions) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS sessions,
            sum(transaction_revenue_no_influencer_campaign.orders) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::numeric AS orders,
            sum(transaction_revenue_no_influencer_campaign.campaign_transaction_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS campaign_transaction_revenue,
            sum(transaction_revenue_no_influencer_campaign.campaign_transaction_net_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS utm_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_campaign_net_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS voucher_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_influencer_campaign_net_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS overall_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_campaign_net_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS no_campaign_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_influencer_campaign_transaction_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS no_influencer_campaign_transaction_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_influencer_campaign_net_revenue) / count(*) OVER (PARTITION BY transaction_revenue_no_influencer_campaign.coupon_code, dim_influencer_enriched_1.campaign_status)::double precision AS no_influencer_campaign_net_revenue
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             JOIN transaction_revenue_no_influencer_campaign ON transaction_revenue_no_influencer_campaign.coupon_code = dim_influencer_enriched_1.coupon_code
          GROUP BY dim_influencer_enriched_1.id, transaction_revenue_no_influencer_campaign.coupon_code, (COALESCE(transaction_revenue_no_influencer_campaign.campaign, 'undefined')), dim_influencer_enriched_1.date, dim_influencer_enriched_1.campaign_status
        ), influencer_cat AS (
         select influencer_name,
		  "date",
		  gross_reach,
		  r_views,
		  engagement,
		  total_costs,
		  influencer_type
         from 
         (
         SELECT dim_influencer_enriched_1.influencer_name,
            -- DISTINCT ON (dim_influencer_enriched_1.influencer_name) dim_influencer_enriched_1.influencer_name,
            dim_influencer_enriched_1.date,
            dim_influencer_enriched_1.gross_reach,
            dim_influencer_enriched_1.r_views,
            dim_influencer_enriched_1.engagement,
            dim_influencer_enriched_1.total_costs,
            COALESCE(influencer_categorization.influencer_type, 'not dertermined') AS influencer_type,
            row_number() over(partition by dim_influencer_enriched_1.influencer_name order by dim_influencer_enriched_1.influencer_name, dim_influencer_enriched_1.date DESC) as id_ranked
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."influencer"."influencer_categorisation" influencer_categorization ON dim_influencer_enriched_1.gross_reach >= influencer_categorization.influencer_type_reach_lower_boundary::double precision AND dim_influencer_enriched_1.gross_reach <= influencer_categorization.influencer_type_reach_upper_boundary::double precision
          WHERE dim_influencer_enriched_1.gross_reach > 0::double precision
        --   ORDER BY dim_influencer_enriched_1.influencer_name, dim_influencer_enriched_1.date DESC
         ) as ranked
         where id_ranked = 1
        ), distinct_coupons AS (
         select coupon_code,
         pk_voucher,
         distinct_coupons
         from
         (SELECT dim_influencer_enriched_1.coupon_code,
            -- DISTINCT ON (dim_influencer_enriched_1.coupon_code) dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.pk_voucher,
            count(DISTINCT dim_influencer_enriched_1.coupon_code) AS distinct_coupons,
            row_number() over(partition by dim_influencer_enriched_1.coupon_code order by dim_influencer_enriched_1.coupon_code, dim_influencer_enriched_1.pk_voucher) as id_ranked
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
          GROUP BY dim_influencer_enriched_1.coupon_code, dim_influencer_enriched_1.pk_voucher) as ranked
         where id_ranked = 1
        ), data_quality_indicator_all AS (
         SELECT dim_influencer_enriched_1.id,
            dim_influencer_enriched_1.reporting_shared,
            COALESCE(count(DISTINCT
                CASE
                    WHEN dim_influencer_enriched_1.reporting_shared = 'y' THEN dim_influencer_enriched_1.reporting_shared
                    ELSE NULL
                END), 0::bigint) AS dq_indicator_yes_all
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
          GROUP BY dim_influencer_enriched_1.id, dim_influencer_enriched_1.reporting_shared
        ), data_quality_indicator_ce AS (
         SELECT dim_influencer_enriched_1.id,
            dim_influencer_enriched_1.reporting_shared,
            COALESCE(count(DISTINCT
                CASE
                    WHEN dim_influencer_enriched_1.reporting_shared = 'y' AND dim_influencer_enriched_1.region = 'central europe' THEN dim_influencer_enriched_1.reporting_shared
                    ELSE NULL
                END), 0::bigint) AS dq_indicator_yes_central_europe
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
          GROUP BY dim_influencer_enriched_1.id, dim_influencer_enriched_1.reporting_shared
        ), data_quality_indicator_se AS (
         SELECT dim_influencer_enriched_1.id,
            dim_influencer_enriched_1.reporting_shared,
            COALESCE(count(DISTINCT
                CASE
                    WHEN dim_influencer_enriched_1.reporting_shared = 'y' AND dim_influencer_enriched_1.region = 'south europe' THEN dim_influencer_enriched_1.reporting_shared
                    ELSE NULL
                END), 0::bigint) AS dq_indicator_yes_south_europe
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
          GROUP BY dim_influencer_enriched_1.id, dim_influencer_enriched_1.reporting_shared
        ), data_quality_indicator_ne AS (
         SELECT dim_influencer_enriched_1.id,
            dim_influencer_enriched_1.reporting_shared,
            COALESCE(count(DISTINCT
                CASE
                    WHEN dim_influencer_enriched_1.reporting_shared = 'y' AND dim_influencer_enriched_1.region = 'north europe' THEN dim_influencer_enriched_1.reporting_shared
                    ELSE NULL
                END), 0::bigint) AS dq_indicator_yes_north_europe
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
          GROUP BY dim_influencer_enriched_1.id, dim_influencer_enriched_1.reporting_shared
        )
 SELECT DISTINCT dim_influencer_enriched.id,
    dim_influencer_enriched.coupon_code,
    dim_influencer_enriched.date,
    dim_influencer_enriched.campaign,
    dim_influencer_enriched.campaign_status,
    dim_influencer_enriched.influencer_name,
    dim_influencer_enriched.gross_reach AS follower,
    dim_influencer_enriched.project_management,
    dim_influencer_enriched.country_shortname,
    dim_influencer_enriched.country,
    dim_influencer_enriched.region,
    dim_influencer_enriched.category,
    dim_influencer_enriched.medium,
    dim_influencer_enriched.medium_type,
    dim_influencer_enriched.content,
    dim_influencer_enriched.quality,
    dim_influencer_enriched.reporting_shared,
    influencer_cat.influencer_type AS size,
    dim_influencer_enriched.total_costs,
    round(NULLIF(dim_influencer_enriched.engagement::numeric::double precision, 0::double precision)::numeric, 0) AS engagement,
    round(NULLIF(dim_influencer_enriched.r_views, 0::double precision)::numeric, 0) AS r_views,
    round(dim_influencer_enriched.estimated_net_revenue, 2) AS nr_target,
    distinct_coupons.distinct_coupons,
    data_quality_indicator_all.dq_indicator_yes_all,
    data_quality_indicator_ce.dq_indicator_yes_central_europe,
    data_quality_indicator_se.dq_indicator_yes_south_europe,
    data_quality_indicator_ne.dq_indicator_yes_north_europe,
    count(DISTINCT dim_influencer_enriched.id) AS posts,
    COALESCE(count(DISTINCT
        CASE
            WHEN dim_influencer_enriched.region = 'central europe' THEN dim_influencer_enriched.region
            ELSE NULL
        END), 0::bigint) AS ce_posts,
    COALESCE(count(DISTINCT
        CASE
            WHEN dim_influencer_enriched.region = 'south europe' THEN dim_influencer_enriched.region
            ELSE NULL
        END), 0::bigint) AS se_posts,
    COALESCE(count(DISTINCT
        CASE
            WHEN dim_influencer_enriched.region = 'north europe' THEN dim_influencer_enriched.region
            ELSE NULL
        END), 0::bigint) AS ne_posts,
    max(dim_influencer_enriched._fivetran_synced) AS max_influencer_data_refresh_date,
    sum(COALESCE(all_influencer_file_campaigns_coupons.users, 0::double precision)) AS users,
    sum(COALESCE(all_influencer_file_campaigns_coupons.new_users, 0::numeric)) AS new_users,
    sum(COALESCE(all_influencer_file_campaigns_coupons.sessions, 0::double precision)) AS sessions,
    sum(COALESCE(all_influencer_file_campaigns_coupons.orders, 0::numeric)) AS orders,
    sum(COALESCE(all_influencer_file_campaigns_coupons.campaign_transaction_revenue, 0::double precision)) AS campaign_transaction_revenue,
    sum(COALESCE(all_influencer_file_campaigns_coupons.utm_net_revenue, 0::double precision)) AS utm_net_revenue,
    sum(COALESCE(all_influencer_file_campaigns_coupons.voucher_net_revenue, 0::double precision)) AS voucher_net_revenue,
    sum(COALESCE(all_influencer_file_campaigns_coupons.overall_net_revenue, 0::double precision)) AS overall_net_revenue,
    sum(COALESCE(all_influencer_file_campaigns_coupons.no_campaign_net_revenue, 0::double precision)) AS no_campaign_net_revenue,
    sum(COALESCE(all_influencer_file_campaigns_coupons.no_influencer_campaign_transaction_revenue, 0::double precision)) AS no_influencer_campaign_transaction_revenue,
    sum(COALESCE(all_influencer_file_campaigns_coupons.no_influencer_campaign_net_revenue, 0::double precision)) AS no_influencer_campaign_net_revenue
   FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched
     LEFT JOIN distinct_coupons USING (pk_voucher)
     LEFT JOIN data_quality_indicator_all USING (id, reporting_shared)
     LEFT JOIN data_quality_indicator_ce USING (id, reporting_shared)
     LEFT JOIN data_quality_indicator_se USING (id, reporting_shared)
     LEFT JOIN data_quality_indicator_ne USING (id, reporting_shared)
     LEFT JOIN influencer_cat ON dim_influencer_enriched.influencer_name = influencer_cat.influencer_name
     LEFT JOIN all_influencer_file_campaigns_coupons USING (id)
  WHERE dim_influencer_enriched.campaign_status = 'done'
  GROUP BY dim_influencer_enriched.id, dim_influencer_enriched.coupon_code, dim_influencer_enriched.date, dim_influencer_enriched.campaign, dim_influencer_enriched.campaign_status, 
  dim_influencer_enriched.influencer_name, dim_influencer_enriched.gross_reach, dim_influencer_enriched.project_management, dim_influencer_enriched.country_shortname, 
  dim_influencer_enriched.country, dim_influencer_enriched.region, dim_influencer_enriched.category, dim_influencer_enriched.medium, dim_influencer_enriched.medium_type, 
  dim_influencer_enriched.content, dim_influencer_enriched.quality, dim_influencer_enriched.reporting_shared, influencer_cat.influencer_type, dim_influencer_enriched.total_costs, 
  (round(NULLIF(dim_influencer_enriched.engagement::numeric::double precision, 0::double precision)::numeric, 0)), (round(NULLIF(dim_influencer_enriched.r_views, 0::double precision)::numeric, 0)), 
  (round(dim_influencer_enriched.estimated_net_revenue, 2)), distinct_coupons.distinct_coupons, data_quality_indicator_all.dq_indicator_yes_all, data_quality_indicator_ce.dq_indicator_yes_central_europe, 
  data_quality_indicator_se.dq_indicator_yes_south_europe, data_quality_indicator_ne.dq_indicator_yes_north_europe