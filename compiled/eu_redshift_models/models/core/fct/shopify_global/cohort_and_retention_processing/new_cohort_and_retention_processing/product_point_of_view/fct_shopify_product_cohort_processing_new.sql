---######################################
---### based view for all cohort-based analyses
---#####################################





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
            product_categorisation.category,
            product_categorisation.subcategory_1,
            product_categorisation.subcategory_2,
            product_categorisation.subcategory_3_clean as subcategory_3,
            product_categorisation_ss_and_bundles.subcategory_4,
            order_line.quantity,
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
                    when order_enriched.shop_country in ('US') THEN 'America'
            	    else 'others'
    		    end) over (partition by order_enriched.customer_id order by order_enriched.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as min_region_per_customer
            ---------
        from
          --shopify_global.fct_order_enriched order_enriched
          "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
          --left join shopify_global.fct_order_line order_line
          left join "airup_eu_dwh"."shopify_global"."fct_order_line" order_line
            on order_enriched.id = order_line.order_id
          --left join shopify_global.seed_shopify_product_categorisation product_categorisation
          left join "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" product_categorisation
            on order_line.sku = product_categorisation.sku
          left join "airup_eu_dwh"."shopify_global"."seed_shopify_product_categorisation_ss_and_bundle_content" product_categorisation_ss_and_bundles
            on product_categorisation.sku = product_categorisation_ss_and_bundles.sku
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
       subcategory_4,
		   nth_order,
		   min_order_per_customer,
       	---## flagging orders with drinking systems
		   max(case when nth_order = 1 and category = 'Hardware' then 1 else 0 end
				) over (partition by order_id) as ss_bundle_init_order
		from
		    all_orders

	)

select
   *
from
    all_orders_enriched
where
  -- removing customers who purchased before the market launch
  min_order_per_customer = 
      case 
        when (country = 'DE' and min_order_per_customer >= '2020-04-01')
        or (country = 'AT' and min_order_per_customer >= '2020-04-01')
        or (country = 'FR' and min_order_per_customer >= '2020-09-01') 
        or (country = 'BE' and min_order_per_customer >= '2020-11-01') 
        or (country = 'NL' and min_order_per_customer >= '2020-11-01') 
        or (country = 'CH' and min_order_per_customer >= '2021-03-01') 
        or (country = 'UK' and min_order_per_customer >= '2021-06-01') 
        or (country = 'IT' and min_order_per_customer >= '2021-09-01') 
        or (country = 'SE' and min_order_per_customer >= '2022-03-01')
        or (country = 'US' and min_order_per_customer >= '2022-06-01')
        then min_order_per_customer
    end