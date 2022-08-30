

  create view "airup_eu_dwh"."zzz_long_test"."fct_shopify_product_direct_retention_first_orders__dbt_tmp" as (
    WITH
    ---######################################
	---### pulling data from the retention processing and prepping it
	---#####################################
	all_orders_content as (
	    select
	    	*
		from
	    	dbt_feldm.fct_shopify_cohort_processing_new
	    -- delete before prod
-- 	    where customer_id = '001a154f791852b017136be0c348d8e5'
	),

    first_order_content as (
        select
        	customer_id as first_order_customer_id,
            order_id as first_order_order_id,
            date(created_at) as first_order_created_at,
            nth_order as first_order_nth_order,
            category as first_order_category,
            subcategory_1 as first_order_subcategory_1,
        	subcategory_2 as first_order_subcategory_2
        from
        	all_orders_content
        where
        	all_orders_content.nth_order = 1
	),

    subsequent_order_content as (
        select
        	customer_id as sub_order_customer_id,
            order_id as sub_order_order_id,
            date(created_at) as sub_order_created_at,
            nth_order as sub_order_nth_order,
            category as sub_order_category,
            subcategory_1 as sub_order_subcategory_1,
        	subcategory_2 as sub_order_subcategory_2
        from
        	all_orders_content
        where
        	all_orders_content.nth_order >= 2
	),

     first_vs_subsequents_content as (
		 select first_order_content.*,
				subsequent_order_content.*,
				case
					when first_order_content.first_order_category = subsequent_order_content.sub_order_category
						and first_order_content.first_order_subcategory_1 =
							subsequent_order_content.sub_order_subcategory_1
						and first_order_content.first_order_subcategory_2 =
							subsequent_order_content.sub_order_subcategory_2
						then 1
					else null
					end as flavour_in_sub,
				case
					when first_order_content.first_order_category = subsequent_order_content.sub_order_category
						and first_order_content.first_order_subcategory_1 =
							subsequent_order_content.sub_order_subcategory_1
						and first_order_content.first_order_subcategory_2 =
							subsequent_order_content.sub_order_subcategory_2
						then datediff(month, first_order_created_at::date,
									  subsequent_order_content.sub_order_created_at::date)
					else null
					end as month_diff
		 from first_order_content
				  left join subsequent_order_content
							on first_order_content.first_order_customer_id =
							   subsequent_order_content.sub_order_customer_id
	 ),

    cohort_size as (
        select
        	first_order_created_at,
            first_order_category,
            first_order_subcategory_1,
            first_order_subcategory_2,
            count(distinct first_order_customer_id) as cohort_size
        from
        	first_order_content
        group by
        	1,2,3,4
	),

     aggregation as (
		select
			a.first_order_created_at,
		    a.first_order_subcategory_2,
		    a.month_diff,
		    b.cohort_size,
		    count(distinct
				case when a.flavour_in_sub = 1 then a.first_order_customer_id
		        else null end
			) as retention_count,
		    (count(distinct
				case when a.flavour_in_sub = 1 then a.first_order_customer_id
		        else null end
			)/cohort_size::float)*100 as retention

		from
			first_vs_subsequents_content a
			left join cohort_size b
				 using (
						first_order_created_at,
            			first_order_category,
            			first_order_subcategory_1,
            			first_order_subcategory_2)
        group by 1,2,3,4
	)

select * from aggregation
where month_diff >= 0
order by first_order_subcategory_2, month_diff asc
  ) with no schema binding;
