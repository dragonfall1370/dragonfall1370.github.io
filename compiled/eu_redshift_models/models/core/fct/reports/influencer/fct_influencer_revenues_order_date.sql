

WITH influencer_voucher_no_campaigns AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country =  (country_system_account_mapping.country_fullname)
          WHERE (dim_influencer_enriched_1.coupon_code IS NOT NULL OR dim_influencer_enriched_1.coupon_code <> 'undefined') AND (dim_influencer_enriched_1.campaign = 'undefined' OR dim_influencer_enriched_1.campaign IS NULL)
        ), influencer_campaigns_no_voucher AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country =  (country_system_account_mapping.country_fullname)
          WHERE dim_influencer_enriched_1.campaign IS NOT NULL AND dim_influencer_enriched_1.campaign <> 'undefined' AND (dim_influencer_enriched_1.coupon_code = 'undefined' OR dim_influencer_enriched_1.coupon_code IS NULL)
        ), influencer_voucher_campaigns AS (
         SELECT DISTINCT COALESCE(country_system_account_mapping.country_grouping, 'other') AS region,
            dim_influencer_enriched_1.coupon_code,
            dim_influencer_enriched_1.campaign
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" country_system_account_mapping ON dim_influencer_enriched_1.country =  (country_system_account_mapping.country_fullname)
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
                    -- rtrim(order_enriched.shipping_address_name, ' ') AS customer_name,
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
            'undefined' AS utm_campaign_name,
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
            -- NULL AS discount_code,
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
            influencer_no_campaign_logic.created_at,
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
          GROUP BY influencer_no_campaign_logic.region, influencer_no_campaign_logic.coupon_code, influencer_no_campaign_logic.created_at
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
            dim_influencer_enriched_1.country,
            all_transactions_union.campaign,
            all_transactions_union.coupon_code,
            all_transactions_union.created_at,
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
             JOIN "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1 ON dim_influencer_enriched_1.campaign = all_transactions_union.campaign AND
                CASE
                    WHEN dim_influencer_enriched_1.coupon_code IS NULL THEN 'undefined'::text
                    ELSE dim_influencer_enriched_1.coupon_code
                END =
                CASE
                    WHEN all_transactions_union.coupon_code IS NULL THEN 'undefined'::text
                    ELSE all_transactions_union.coupon_code
                END
          WHERE dim_influencer_enriched_1.campaign_status = 'done'
          GROUP BY all_transactions_union.region, dim_influencer_enriched_1.country, all_transactions_union.campaign, all_transactions_union.coupon_code, all_transactions_union.created_at
        UNION ALL
         SELECT transaction_revenue_no_influencer_campaign.region,
            dim_influencer_enriched_1.country,
            COALESCE(transaction_revenue_no_influencer_campaign.campaign, 'undefined') AS campaign,
            transaction_revenue_no_influencer_campaign.coupon_code,
            transaction_revenue_no_influencer_campaign.created_at,
            sum(transaction_revenue_no_influencer_campaign.users) AS users,
            sum(transaction_revenue_no_influencer_campaign.new_users) AS new_users,
            sum(transaction_revenue_no_influencer_campaign.sessions) AS sessions,
            sum(transaction_revenue_no_influencer_campaign.orders) AS orders,
            sum(transaction_revenue_no_influencer_campaign.campaign_transaction_revenue) AS campaign_transaction_revenue,
            sum(transaction_revenue_no_influencer_campaign.campaign_transaction_net_revenue) AS utm_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_campaign_net_revenue) AS voucher_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_influencer_campaign_net_revenue) AS overall_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_campaign_net_revenue) AS no_campaign_net_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_influencer_campaign_transaction_revenue) AS no_influencer_campaign_transaction_revenue,
            sum(transaction_revenue_no_influencer_campaign.no_influencer_campaign_net_revenue) AS no_influencer_campaign_net_revenue
           FROM "airup_eu_dwh"."influencer"."dim_influencer_enriched" dim_influencer_enriched_1
             JOIN transaction_revenue_no_influencer_campaign ON transaction_revenue_no_influencer_campaign.coupon_code = dim_influencer_enriched_1.coupon_code
          WHERE dim_influencer_enriched_1.campaign_status = 'done'
          GROUP BY transaction_revenue_no_influencer_campaign.region, dim_influencer_enriched_1.country, (COALESCE(transaction_revenue_no_influencer_campaign.campaign, 'undefined')), transaction_revenue_no_influencer_campaign.coupon_code, transaction_revenue_no_influencer_campaign.created_at
        )
 SELECT transactions_revenue_final_aggregation.region,
    transactions_revenue_final_aggregation.country,
    transactions_revenue_final_aggregation.campaign,
    transactions_revenue_final_aggregation.coupon_code,
    transactions_revenue_final_aggregation.created_at,
    transactions_revenue_final_aggregation.users,
    transactions_revenue_final_aggregation.new_users,
    transactions_revenue_final_aggregation.sessions,
    transactions_revenue_final_aggregation.orders,
    transactions_revenue_final_aggregation.campaign_transaction_revenue,
    transactions_revenue_final_aggregation.utm_net_revenue,
    transactions_revenue_final_aggregation.voucher_net_revenue,
    transactions_revenue_final_aggregation.overall_net_revenue,
    transactions_revenue_final_aggregation.no_campaign_net_revenue,
    transactions_revenue_final_aggregation.no_influencer_campaign_transaction_revenue,
    transactions_revenue_final_aggregation.no_influencer_campaign_net_revenue
   FROM transactions_revenue_final_aggregation