

with 
    campaign_v1 as (
        WITH
    ---######################################
	---### pulling data from the retention processing and prepping it
	---#####################################
	data_prep as (
	    select
	    	cohort.customer_id,
	        cohort.order_id,
	        cohort.created_at,
	        cohort.country,
			cohort.region,
	        cohort.net_revenue_2,
			cohort.net_revenue_2_usd,
	        cohort.nth_order,
	        cohort.min_order_per_customer,
	        cohort.max_order_per_customer,
	        cohort.cohort,
	        cohort.month_diff,
	        case
                when cohort.ss_bundle_init = 1 then 'Yes'
                when cohort.ss_bundle_init = 0 then 'No'
            end ss_bundle_init,
	        cohort.active_customer,
	        cohort.new_customer_churned,
	        cohort.returning_customer_churned,
			case
                when cohort.used_discount_1st_order = 1 then 'Yes'
                when cohort.used_discount_1st_order = 0 then 'No'
            end used_discount_1st_order,
			cohort.opt_to_email,
	        case when campaign_orders.customer_id is not null then 1 else 0 end as is_campaign_customer,
			sum(cohort.quantity) as quantity
		from
	    	"airup_eu_dwh"."dbt_feldm"."fct_shopify_cohort_processing_new" cohort
	    	LEFT JOIN "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" campaign_orders
        		on cohort.customer_id = campaign_orders.customer_id
	    		and campaign_orders.campaign_name = 'l-de_n-off_k-free-strap-v1'
        where
        	cohort.min_order_per_customer > '2021-10-01'
		group by
			1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
	    having is_campaign_customer = 1

	),

	---######################################
	---### cohort-based metrics (e.g., it's size, active customers)
	---#####################################
	cohort_overall_metrics as (
		 select
			cohort,
			country,
			region,
            ss_bundle_init,
			used_discount_1st_order,
			opt_to_email,
			coalesce(count(distinct customer_id),0) as cohort_size,
			coalesce(sum(active_customer),0) as active_customer,
			coalesce(sum(new_customer_churned),0) as cohort_new_customers_churned,
			coalesce(sum(returning_customer_churned),0) as cohort_returning_customers_churned
		 from
			data_prep
		 where
			nth_order = 1
		 group by 1,2,3,4,5,6
		 ),

	---######################################
	---### calculating metrics per datediffs (months)
	---#####################################
	cohort_monthly_metrics as (
		select
			cohort,
			country,
			region,
      		ss_bundle_init,
			month_diff,
			used_discount_1st_order,
			opt_to_email,
      		date(date_trunc('month', created_at)) as date,
			--- customers (all and with subsequent orders)
			count(distinct customer_id) as total_customers,
			count(distinct case when nth_order <> 1 then customer_id end
				) as returning_customers, -- to be divided by cohort_size
			--- orders
			count(distinct order_id) as total_orders,
			count(distinct case when nth_order <> 1 then order_id end
				) as subsequent_orders,
			--- revenue
			sum(net_revenue_2) as total_revenue,
			sum(net_revenue_2_usd) as total_revenue_usd,
			sum(case when nth_order = 1 then net_revenue_2 end
				) as seeding_revenue,
			sum(case when nth_order = 1 then net_revenue_2_usd end
				) as seeding_revenue_usd,
			sum(case when nth_order <> 1 then net_revenue_2 end
				) as recurring_revenue,
			sum(case when nth_order <> 1 then net_revenue_2_usd end
				) as recurring_revenue_usd,
			--- item quantity
			sum(quantity) as total_item_quantity,
			sum(case when nth_order <> 1 then quantity end
				) as subsequent_item_quantity
		from
			data_prep
		group by
			1,2,3,4,5,6,7,8
		),

	final as (
		select
			-- mapping dimensions
			cohort_mapping.cohort,
			cohort_mapping.country,
			cohort_overall_metrics.region,
			-- first overall cte metrics
			cohort_overall_metrics.ss_bundle_init,
			cohort_overall_metrics.used_discount_1st_order,
			cohort_overall_metrics.opt_to_email,
			cohort_overall_metrics.cohort_size,
			cohort_overall_metrics.active_customer,
			cohort_overall_metrics.cohort_new_customers_churned,
			cohort_overall_metrics.cohort_returning_customers_churned,
			-- second month-based cte metrics
			cohort_monthly_metrics.month_diff,
			cohort_monthly_metrics.date,
			cohort_monthly_metrics.total_customers,
			cohort_monthly_metrics.returning_customers,
			cohort_monthly_metrics.total_orders,
			cohort_monthly_metrics.subsequent_orders,
			cohort_monthly_metrics.total_revenue,
			cohort_monthly_metrics.total_revenue_usd,
			cohort_monthly_metrics.seeding_revenue,
			cohort_monthly_metrics.seeding_revenue_usd,
			cohort_monthly_metrics.recurring_revenue,
			cohort_monthly_metrics.recurring_revenue_usd,
			cohort_monthly_metrics.total_item_quantity,
			cohort_monthly_metrics.subsequent_item_quantity
		from
			dbt_feldm.dim_shopify_cohort_mapping cohort_mapping
			left join cohort_overall_metrics
				on cohort_mapping.country = cohort_overall_metrics.country
				and cohort_mapping.cohort = cohort_overall_metrics.cohort
	       	left join cohort_monthly_metrics
	    		on cohort_overall_metrics.cohort = cohort_monthly_metrics.cohort
	    		and cohort_overall_metrics.country = cohort_monthly_metrics.country
				and cohort_overall_metrics.ss_bundle_init = cohort_monthly_metrics.ss_bundle_init
				and cohort_overall_metrics.used_discount_1st_order = cohort_monthly_metrics.used_discount_1st_order
				and cohort_overall_metrics.opt_to_email = cohort_monthly_metrics.opt_to_email
		)

select
	*
from
	final
    ),

    campaign_v2 as(
        WITH
    ---######################################
	---### pulling data from the retention processing and prepping it
	---#####################################
	data_prep as (
	    select
	    	cohort.customer_id,
	        cohort.order_id,
	        cohort.created_at,
	        cohort.country,
			cohort.region,
	        cohort.net_revenue_2,
			cohort.net_revenue_2_usd,
	        cohort.nth_order,
	        cohort.min_order_per_customer,
	        cohort.max_order_per_customer,
	        cohort.cohort,
	        cohort.month_diff,
	        case
                when cohort.ss_bundle_init = 1 then 'Yes'
                when cohort.ss_bundle_init = 0 then 'No'
            end ss_bundle_init,
	        cohort.active_customer,
	        cohort.new_customer_churned,
	        cohort.returning_customer_churned,
			case
                when cohort.used_discount_1st_order = 1 then 'Yes'
                when cohort.used_discount_1st_order = 0 then 'No'
            end used_discount_1st_order,
			cohort.opt_to_email,
	        case when campaign_orders.customer_id is not null then 1 else 0 end as is_campaign_customer,
			sum(cohort.quantity) as quantity
		from
	    	"airup_eu_dwh"."dbt_feldm"."fct_shopify_cohort_processing_new" cohort
	    	LEFT JOIN "airup_eu_dwh"."dbt_feldm"."dim_offline_campaign_orders" campaign_orders
        		on cohort.customer_id = campaign_orders.customer_id
	    		and campaign_orders.campaign_name = 'l-de_n-off_k-free-strap-v2'
        where
        	cohort.min_order_per_customer > '2021-10-01'
		group by
			1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
	    having is_campaign_customer = 1

	),

	---######################################
	---### cohort-based metrics (e.g., it's size, active customers)
	---#####################################
	cohort_overall_metrics as (
		 select
			cohort,
			country,
			region,
            ss_bundle_init,
			used_discount_1st_order,
			opt_to_email,
			coalesce(count(distinct customer_id),0) as cohort_size,
			coalesce(sum(active_customer),0) as active_customer,
			coalesce(sum(new_customer_churned),0) as cohort_new_customers_churned,
			coalesce(sum(returning_customer_churned),0) as cohort_returning_customers_churned
		 from
			data_prep
		 where
			nth_order = 1
		 group by 1,2,3,4,5,6
		 ),

	---######################################
	---### calculating metrics per datediffs (months)
	---#####################################
	cohort_monthly_metrics as (
		select
			cohort,
			country,
			region,
      		ss_bundle_init,
			month_diff,
			used_discount_1st_order,
			opt_to_email,
      		date(date_trunc('month', created_at)) as date,
			--- customers (all and with subsequent orders)
			count(distinct customer_id) as total_customers,
			count(distinct case when nth_order <> 1 then customer_id end
				) as returning_customers, -- to be divided by cohort_size
			--- orders
			count(distinct order_id) as total_orders,
			count(distinct case when nth_order <> 1 then order_id end
				) as subsequent_orders,
			--- revenue
			sum(net_revenue_2) as total_revenue,
			sum(net_revenue_2_usd) as total_revenue_usd,
			sum(case when nth_order = 1 then net_revenue_2 end
				) as seeding_revenue,
			sum(case when nth_order = 1 then net_revenue_2_usd end
				) as seeding_revenue_usd,
			sum(case when nth_order <> 1 then net_revenue_2 end
				) as recurring_revenue,
			sum(case when nth_order <> 1 then net_revenue_2_usd end
				) as recurring_revenue_usd,
			--- item quantity
			sum(quantity) as total_item_quantity,
			sum(case when nth_order <> 1 then quantity end
				) as subsequent_item_quantity
		from
			data_prep
		group by
			1,2,3,4,5,6,7,8
		),

	final as (
		select
			-- mapping dimensions
			cohort_mapping.cohort,
			cohort_mapping.country,
			cohort_overall_metrics.region,
			-- first overall cte metrics
			cohort_overall_metrics.ss_bundle_init,
			cohort_overall_metrics.used_discount_1st_order,
			cohort_overall_metrics.opt_to_email,
			cohort_overall_metrics.cohort_size,
			cohort_overall_metrics.active_customer,
			cohort_overall_metrics.cohort_new_customers_churned,
			cohort_overall_metrics.cohort_returning_customers_churned,
			-- second month-based cte metrics
			cohort_monthly_metrics.month_diff,
			cohort_monthly_metrics.date,
			cohort_monthly_metrics.total_customers,
			cohort_monthly_metrics.returning_customers,
			cohort_monthly_metrics.total_orders,
			cohort_monthly_metrics.subsequent_orders,
			cohort_monthly_metrics.total_revenue,
			cohort_monthly_metrics.total_revenue_usd,
			cohort_monthly_metrics.seeding_revenue,
			cohort_monthly_metrics.seeding_revenue_usd,
			cohort_monthly_metrics.recurring_revenue,
			cohort_monthly_metrics.recurring_revenue_usd,
			cohort_monthly_metrics.total_item_quantity,
			cohort_monthly_metrics.subsequent_item_quantity
		from
			dbt_feldm.dim_shopify_cohort_mapping cohort_mapping
			left join cohort_overall_metrics
				on cohort_mapping.country = cohort_overall_metrics.country
				and cohort_mapping.cohort = cohort_overall_metrics.cohort
	       	left join cohort_monthly_metrics
	    		on cohort_overall_metrics.cohort = cohort_monthly_metrics.cohort
	    		and cohort_overall_metrics.country = cohort_monthly_metrics.country
				and cohort_overall_metrics.ss_bundle_init = cohort_monthly_metrics.ss_bundle_init
				and cohort_overall_metrics.used_discount_1st_order = cohort_monthly_metrics.used_discount_1st_order
				and cohort_overall_metrics.opt_to_email = cohort_monthly_metrics.opt_to_email
		)

select
	*
from
	final   
    ),

    final as(
        select *, 'l-de_n-off_k-free-strap-v1' as campaign_name from campaign_v1 
        union all
        select *, 'l-de_n-off_k-free-strap-v2' as campaign_name from campaign_v2
    )

select * from final