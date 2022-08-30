 

with clv_prediction as 
( -- selecting the customer ids which would exclude the ones coming from the inner pass.
select
		*
from
		"airup_eu_dwh"."clv"."prediction_inference"
where
		customer_id not in 
( -- selecting all the customer ids which should be excluded from the model because the log normal loss method in the predictive model pushes the predicted clv values to a very high number.
  -- These are unreasonable high numbers which the model cannot figure out given less historical data available.
	select
			customer_id
	from
			"airup_eu_dwh"."clv"."prediction_inference"
	where
((predicted_clv>y_true * 4
			and y_true > 50)
		or (predicted_clv>y_true * 8
			and y_true <= 50
			and y_true > 0)
		or (y_true = 0
			and predicted_clv - y_true > 100)))
),
order_enriched_columns as 
	(
select
	*
from
	( -- This inner pass is to select the customer address and zip codes which would be needed for descriptive analysis of customer or customer cluster.
      -- The rank logic is just to pick the later shipping address for customers who might have multiple shipping address for some of their historical orders.
	select
		distinct customer_id,
		shipping_address_city,
		shipping_address_longitude,
		shipping_address_latitude,
		shipping_address_zip,
		-- country_fullname,
		-- country_grouping,
		rank() over (partition by customer_id
	order by
		created_at desc) as latest_cust_order
	from
		"airup_eu_dwh"."shopify_global"."fct_order_enriched" foe) pa1
join clv.prediction_inference
		using (customer_id)
where
	pa1.latest_cust_order = 1
	),
rfm_segmentation_columns as 
( -- This cte is just to select some of the variables from rfm segmentation model by joining them on customer_ids.
select
	customer_id,
	recency,
	frequency,
	country as country_fullname,
	region as country_grouping,
	net_revenue,
	customer_segment
from
	"airup_eu_dwh"."clv"."fct_rfm_segmentation" frs 
) -- Final query output which combines all the above cte joining them on customer ids.
select
	customer_id,
	clv_prediction.predicted_clv,
	clv_prediction.y_true as actual_sales,
	clv_prediction.prob_alive,
	clv_prediction.churned,
	clv_prediction.predicted_churn_25,
	clv_prediction.predicted_churn_thresh,
	shipping_address_city,
	shipping_address_longitude,
	shipping_address_latitude,
	shipping_address_zip,
	country_fullname,
	country_grouping,
	recency,
	frequency,
	net_revenue,
	customer_segment
from
	clv_prediction
join order_enriched_columns
		using (customer_id)
join rfm_segmentation_columns using (customer_id)