---######################################
---### the following view is calculating
-- ### direct product retention with first order POV
---#####################################



WITH
    ---######################################
	---### pulling relevant data from the shopify cohort processing table
	---#####################################
	all_orders_content as (
	    select
			customer_id,
			order_id,
			created_at,
			country,
			region,
			net_revenue_2,
			-- in this view we are analysis products on the subcategory_4 level (not SS/bundle)
			case when category = 'Hardware' then 'Flavour' else category end as category,
			case when subcategory_4 is NULL then subcategory_3 else subcategory_4 end as product,
			nth_order,
			min_order_per_customer,
			ss_bundle_init_order
		from
             "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_cohort_processing_new"
-- 	    	dbt_feldm.fct_shopify_product_cohort_processing_new
	    group by 1,2,3,4,5,6,7,8,9,10,11
	),

    ---######################################
	---### separating the first order and renaming columns for easier maintenance
	---#####################################
    first_order_content as (
        select
            customer_id,
            order_id as first_order_id,
            region as first_region,
            country as first_country,
        	net_revenue_2 as first_net_revenue_2,
            date(created_at) as first_created_at,
			ss_bundle_init_order as first_ss_bundle_init_order,
            category as first_category,
            product as first_product
        from
        	all_orders_content
        where
        	all_orders_content.nth_order = 1
	),

---######################################
---### separating the first order and renaming columns for easier maintenance
---#####################################
    subsequent_order_content as (
        select
        	customer_id as sub_customer_id,
            order_id as sub_order_id,
            date(created_at) as sub_created_at,
            nth_order as sub_nth_order,
            category as sub_category,
            product as sub_product
        from
        	all_orders_content
        where
        	all_orders_content.nth_order >= 2
	),

---######################################
---### flagging direct returns and calculating month_diff
---#####################################
     first_vs_subsequents_content as (
		 select first_order_content.*,
				subsequent_order_content.*,
		    	-- flagging direct returns based on subcategory_4 match
				case
				    when sub_product is not null
				    and first_product = sub_product
				    then 1 else 0
				end as product_in_sub_order,
				datediff(month, first_created_at::date, subsequent_order_content.sub_created_at::date) as month_diff
		 from first_order_content
				  left join subsequent_order_content
							on first_order_content.customer_id = subsequent_order_content.sub_customer_id
	 ),

---######################################
---### calculation of cohort size for flavours, accessories and others
---### aggregation of flavours, accessories and others
---#####################################

    cohort_size as (
        select
        	first_created_at,
            first_country,
            first_region,
 			first_product,
			first_ss_bundle_init_order,
            count(distinct customer_id) as cohort_size
        from
        	first_order_content
        group by
        	1,2,3,4,5
	),

    final as (
		select
			spine.date_spine as date,
			spine.country_spine as country,
			spine.region_spine as region,
			spine.category_spine as category,
			spine.product_spine as product,
			spine.first_ss_bundle_init_order_spine as first_ss_bundle_init_order,
			spine.month_diff_spine as month_diff,
		    b.cohort_size,
		    count(distinct
				case when a.product_in_sub_order = 1 then a.customer_id end
				) as direct_retention_count,
		    count(distinct a.customer_id
				) as indirect_retention_count
		from "airup_eu_dwh"."dbt_feldm"."dim_shopify_product_cohort_spine" spine
			left join first_vs_subsequents_content a
				on spine.date_spine = a.first_created_at
				and spine.country_spine = a.first_country
				and spine.region_spine = a.first_region
				and spine.category_spine = a.first_category
				and spine.product_spine = a.first_product
				and spine.first_ss_bundle_init_order_spine = a.first_ss_bundle_init_order
				and spine.month_diff_spine = a.month_diff
			left join cohort_size b
			    on spine.date_spine = b.first_created_at
			    and spine.country_spine = b.first_country
			    and spine.region_spine = b.first_region
				and spine.first_ss_bundle_init_order_spine = b.first_ss_bundle_init_order
			    and spine.product_spine = b.first_product
        group by 1,2,3,4,5,6,7,8
	)

select
	*
from
	final
where
     dateadd(month, month_diff::int, date_trunc('month', date)) <= date_trunc('month', current_date)