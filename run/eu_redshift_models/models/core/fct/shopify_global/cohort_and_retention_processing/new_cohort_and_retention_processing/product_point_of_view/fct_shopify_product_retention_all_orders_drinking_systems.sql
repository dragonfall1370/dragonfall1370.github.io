

  create view "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_retention_all_orders_drinking_systems__dbt_tmp" as (
    ---######################################
	---### the following view is calculating
	-- ### direct and indirect product-based retention (all orders)
	---#####################################

 	 

	WITH
		---######################################
		---### pulling relevant data from the shopify cohort processing table
        ---### and transforming the category to fit the subcategory_4 logic
		---#####################################
		all_orders_content as (
			select
				customer_id,
				order_id,
				created_at::date,
				country,
				region,
				net_revenue_2,
				-- in this view we are analysis products on the subcategory_3 level (SS/bundle)
				category,
				subcategory_3 as product,
				ss_bundle_init_order,
				nth_order,
				min_order_per_customer::date
			from
 	             "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_cohort_processing_new"
--				dbt_feldm.fct_shopify_product_cohort_processing_new
			group by 1,2,3,4,5,6,7,8,9,10,11
		),

		---######################################
		---### identfying the first order date per flavour
        ---### and adding a flag in case it was purchased with drinking system
		---#####################################
		first_order_per_product as (
			select
				a.customer_id,
				region as first_region,
				country as first_country,
				category as first_category,
				product as first_product,
				min(date(a.created_at)) as first_created_at
			from
				all_orders_content a
			group by 1,2,3,4,5
		),

		prep as (select customer_id, created_at, ss_bundle_init_order from all_orders_content group by 1,2,3),

		first_order_per_product_enhanced as (
			select
				a.customer_id,
				a.first_region,
				a.first_country,
				a.first_category,
				a.first_product,
				a.first_created_at,
				b.ss_bundle_init_order as first_ss_bundle_init_order
			from
				first_order_per_product a
				left join prep b
					on a.customer_id = b.customer_id
					and a.first_created_at = b.created_at
		),

	---######################################
	---### joining the min_flavour data with all subsequent orders
	---#####################################
		first_order_per_product_and_subsequent_orders as (
			select
				first_order_per_product_enhanced.customer_id,
				first_order_per_product_enhanced.first_country,
				first_order_per_product_enhanced.first_region,
				first_order_per_product_enhanced.first_category,
				first_order_per_product_enhanced.first_product,
				first_order_per_product_enhanced.first_created_at,
				first_order_per_product_enhanced.first_ss_bundle_init_order,
				all_orders_content.order_id,
				all_orders_content.category,
				all_orders_content.product,
				all_orders_content.nth_order,
				case
					when product is not null
					and first_product = product
					then 1 else 0
				end as product_in_sub_order,
				datediff(month,first_order_per_product_enhanced.first_created_at::date, all_orders_content.created_at::date) as month_diff
			from
				first_order_per_product_enhanced
				left join all_orders_content
					on first_order_per_product_enhanced.customer_id = all_orders_content.customer_id
			where
				all_orders_content.created_at::date > first_order_per_product_enhanced.first_created_at::date
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
				first_order_per_product_enhanced
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
				left join first_order_per_product_and_subsequent_orders a
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
	and	category = 'Hardware'
  ) with no schema binding;
