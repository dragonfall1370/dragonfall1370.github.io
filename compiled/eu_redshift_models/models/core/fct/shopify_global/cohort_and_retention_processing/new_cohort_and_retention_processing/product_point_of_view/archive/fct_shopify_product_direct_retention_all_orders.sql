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
	    where customer_id = '001a154f791852b017136be0c348d8e5'
	),

    first_order_per_flavour as (
        select
        	customer_id as first_flavour_customer_id,
            category as first_flavour_category,
            subcategory_1 as first_flavour_subcategory_1,
        	subcategory_2 as first_flavour_subcategory_2,
            date(min(created_at)) as first_flavour_date
        from
        	all_orders_content
		group by 1,2,3,4
	),

    first_flavour_order_and_subsequet as (
        select
            first_flavour_customer_id,
            first_flavour_category,
            first_flavour_subcategory_1,
        	first_flavour_subcategory_2,
            first_flavour_date,
        	all_orders_content.customer_id,
            all_orders_content.order_id,
            all_orders_content.category,
            all_orders_content.subcategory_1,
        	all_orders_content.subcategory_2,
        	datediff(month, first_flavour_date::date, all_orders_content.created_at::date) as month_diff
    	from
        	first_order_per_flavour
        	left join all_orders_content
        		on first_order_per_flavour.first_flavour_customer_id = all_orders_content.customer_id
        where
        	date(all_orders_content.created_at) > first_order_per_flavour.first_flavour_date
	)

     aggregation as (
		select
			a.first_order_created_at,
		    a.first_order_subcategory_2,
		    a.month_diff,
		    b.cohort_size,
		    count(distinct a.first_order_customer_id
			) as retention_count,
		    (count(distinct a.first_order_customer_id)/cohort_size::float)*100 as retention
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