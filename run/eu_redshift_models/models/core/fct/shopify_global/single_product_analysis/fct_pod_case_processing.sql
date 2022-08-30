

  create  table
    "airup_eu_dwh"."dbt_nhamdao"."fct_pod_case_processing__dbt_tmp"
    
    
    
  as (
    ---######################################
---### based view for pod_case analysis
---### For customers who bought pod case, the first_order_date is the first date customers bought the pod case (regardless of whether it's the first order or not)
---### For customers who haven't bought pod case, the first_order_date is the date customer placed the first order
---#####################################





with all_orders_content as (select
	customer_id,
	order_id,
	country,
	region,
	net_revenue_2,
    quantity,
	subcategory_3 as product,
	nth_order,
	created_at,
	category,
	max(case when subcategory_3 = 'Pod Case' then 1 else 0 end) over (partition by customer_id) as include_pod_case,
	min(created_at) over (partition by customer_id) as first_order_date,
	count(*) over(partition by order_id) as number_of_distinct_product
from "airup_eu_dwh"."dbt_nhamdao"."fct_order_product_detail"),
first_order_pod_case as (
			select
				a.customer_id,
				min(a.created_at) as pod_case_first_created_at
			from
				all_orders_content a
			where product = 'Pod Case'
			group by 1
			), 
first_and_sub_order_pod_case as 
(select
				all_orders_content.customer_id,
				all_orders_content.country,
				all_orders_content.region,
				first_order_pod_case.pod_case_first_created_at as first_order_date,
				dense_rank() 
				      over (partition by all_orders_content.customer_id order by all_orders_content.created_at)  as nth_order,
				all_orders_content.order_id,
				all_orders_content.category,
				all_orders_content.product,
				all_orders_content.net_revenue_2,
                all_orders_content.quantity,
				all_orders_content.include_pod_case,
				all_orders_content.number_of_distinct_product,
				all_orders_content.created_at,
				datediff(month,first_order_pod_case.pod_case_first_created_at::date, all_orders_content.created_at::date) as month_diff,
				datediff(day,first_order_pod_case.pod_case_first_created_at::date, all_orders_content.created_at::date) as day_diff
			from first_order_pod_case				 
				left join all_orders_content
					on first_order_pod_case.customer_id = all_orders_content.customer_id
				and	all_orders_content.created_at >= first_order_pod_case.pod_case_first_created_at
		) ,
		previous_order_pod_case as 
(select
				all_orders_content.customer_id,
				all_orders_content.country,
				all_orders_content.region,
				first_order_pod_case.pod_case_first_created_at as first_order_date,
				- dense_rank() 
				      over (partition by all_orders_content.customer_id order by all_orders_content.created_at)  as nth_order,
				all_orders_content.order_id,
				all_orders_content.category,
				all_orders_content.product,
				all_orders_content.net_revenue_2,
                all_orders_content.quantity,
				all_orders_content.include_pod_case,
				all_orders_content.number_of_distinct_product,
				all_orders_content.created_at,
				datediff(month,first_order_pod_case.pod_case_first_created_at::date, all_orders_content.created_at::date) as month_diff,
				datediff(day,first_order_pod_case.pod_case_first_created_at::date, all_orders_content.created_at::date) as day_diff
			from first_order_pod_case				 
				left join all_orders_content
					on first_order_pod_case.customer_id = all_orders_content.customer_id
				and	all_orders_content.created_at < first_order_pod_case.pod_case_first_created_at
				where all_orders_content.customer_id is not null
		),
first_and_sub_order_NOT_pod_case as 
(select
				all_orders_content.customer_id,
				all_orders_content.country,
				all_orders_content.region,
				all_orders_content.first_order_date,
				all_orders_content.nth_order,
				all_orders_content.order_id,
				all_orders_content.category,
				all_orders_content.product,
				all_orders_content.net_revenue_2,
                all_orders_content.quantity,
				all_orders_content.include_pod_case,
				all_orders_content.number_of_distinct_product,
				all_orders_content.created_at,
				datediff(month,all_orders_content.first_order_date::date, all_orders_content.created_at::date) as month_diff,
				datediff(day,all_orders_content.first_order_date::date,all_orders_content.created_at::date) as day_diff
			from all_orders_content
			where include_pod_case = 0), 
first_and_sub_order_pod_case_drinking_system as 		
(select *,max(case when nth_order = 1 and category = 'Hardware' then 1 else 0 end
				) over (partition by customer_id) as first_purchase_incl_drinking_system from first_and_sub_order_pod_case),
previous_order_pod_case_drinking_system as
(select previous_order_pod_case.*,first_and_sub_order_pod_case_drinking_system.first_purchase_incl_drinking_system from previous_order_pod_case 
left join first_and_sub_order_pod_case_drinking_system
on previous_order_pod_case.customer_id = first_and_sub_order_pod_case_drinking_system.customer_id),
summary as (select * from first_and_sub_order_pod_case_drinking_system
union 
select * from previous_order_pod_case_drinking_system
union 
select *,max(case when nth_order = 1 and category = 'Hardware' then 1 else 0 end
				) over (partition by customer_id) as first_purchase_incl_drinking_system from first_and_sub_order_NOT_pod_case)
select *, dense_rank() 
				      over (partition by customer_id order by created_at)  as rank_order_per_cust from summary
  );