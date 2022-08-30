
with

	---######################################
	---### joining all the needed tables and getting all the needed fields (e.g., nth_order, min/max order date)
	---#####################################
    all_orders as (
        select
            order_enriched.customer_id,
		    order_enriched.id as order_id,
            order_enriched.created_at,
            order_enriched.net_revenue_2,
			order_line.quantity,
            product_categorisation.category,
            product_categorisation.subcategory_1,
            product_categorisation.subcategory_2,
            product_categorisation.subcategory_3,
            dense_rank() 
				      over (partition by order_enriched.customer_id order by order_enriched.created_at)  as nth_order,
        	min(order_enriched.created_at) 
				      over (partition by order_enriched.customer_id order by order_enriched.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as min_order_per_customer,
            first_value(order_enriched.shop_country) 
              over (partition by order_enriched.customer_id order by order_enriched.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as min_country_per_customer,
            -- todo: migrate upstream
            first_value(case
				    when order_enriched.shop_country in ('AT','DE','CH') THEN 'Central Europe'
                    when order_enriched.shop_country in ('NL','UK','BE','SE') THEN 'North Europe'
                    when order_enriched.shop_country in ('FR','IT') THEN 'South Europe'
            	    else 'others'
    		    end) over (partition by order_enriched.customer_id order by order_enriched.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as min_region_per_customer,
            ---------
        	  max(order_enriched.created_at) 
				      over (partition by order_enriched.customer_id order by order_enriched.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as max_order_per_customer,
            date(date_trunc('month', min(order_enriched.created_at) 
				      over (partition by order_enriched.customer_id order by order_enriched.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) as cohort,
            datediff(month, cohort::date, order_enriched.created_at::date) as month_diff
        from
          --shopify_global.fct_order_enriched order_enriched
          "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          --left join shopify_global.fct_order_line order_line
          left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line
            on order_enriched.id = order_line.order_id
          --left join shopify_global.seed_shopify_product_categorisation product_categorisation
          left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" product_categorisation
            on order_line.sku = product_categorisation.sku
        where order_enriched.financial_status in ('paid', 'partially_refunded')
    ),



	---######################################
	---### flagging different customer types based on the defined logic
	---#####################################
    all_orders_enriched as (
		select
		   customer_id,
		   order_id,
		   created_at,
		   min_country_per_customer as country,
		   min_region_per_customer as region,
		   net_revenue_2,
		   quantity,
		   category,
		   subcategory_1,
		   subcategory_2,
           subcategory_3,
		   nth_order,
		   min_order_per_customer,
		   max_order_per_customer,
		   cohort,
		   month_diff,

		   ---## flagging customers who are eligible for retention calculation
		   max(case when nth_order = 1 and category = 'Hardware' then 1 else 0 end
				) over (partition by customer_id) as ss_bundle_init, --retention_eligible

		   ---## flagging customers who are still active
		   case when datediff(day, max_order_per_customer::date, current_date) <= 90 then 1 else 0 end
		   		as active_customer,

		   ---## flagging customers who churned after their first order (churned new customers); todo: update the number of days; needs more testing
		   max(case when
		       datediff(day, min_order_per_customer::date, max_order_per_customer::date) = 0 -- checking if they only made one order
		       or datediff(day, min_order_per_customer::date, (case when nth_order = 2 then created_at::date end)) > 150 -- checking if customer made the second order too late and churned aleardy
		       then 1 end) over (partition by customer_id) as new_customer_churned,

		   ---## flagging customers who churned after returning at least once todo: update the number of days; needs more testing
		   max(case when
		       datediff(day, min_order_per_customer::date, max_order_per_customer::date) > 0 -- checking if they made more than one order
		       and datediff(day, max_order_per_customer::date, current_date) > 150 -- checking if customer made the second order too late and churned aleardy
		       then 1 end) over (partition by customer_id) as returning_customer_churned
		from
		    all_orders

	)

select
   *
from
    all_orders_enriched