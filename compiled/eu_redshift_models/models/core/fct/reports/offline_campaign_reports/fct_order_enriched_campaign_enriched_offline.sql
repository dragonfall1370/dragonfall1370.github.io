

WITH
    -- ################################
    -- identifying orders_id with campaign UTM parameters
	-- comment Tom: ... with at least one campaign UTM parameter
    -- ################################
    campaign_orders_by_utm as (
        select
            dim_order_url_tag.order_id,
            max(
                case when dim_order_url_tag.key = 'utm_campaign' then dim_order_url_tag.value end
                    ) over (partition by dim_order_url_tag.order_id)
            as campaign_name
        from
            -- TODO replace with a source
            shopify_global.dim_order_url_tag
		-- next 3 lines dbt style
		--        where
		--            (dim_order_url_tag.key = 'utm_source' and dim_order_url_tag.value in )
		--            or (dim_order_url_tag.key = 'utm_medium' and dim_order_url_tag.value in )
		--            or (dim_order_url_tag.key = 'utm_campaign' and dim_order_url_tag.value in )
		-- next 3 lines no DBT style
        WHERE dim_order_url_tag.key = 'utm_source' AND (dim_order_url_tag.value in ('retail', 'other campaings UTM')) 
 	        OR dim_order_url_tag.key = 'utm_medium' AND (dim_order_url_tag.value in ('flyer', 'other campaings UTM')) 
	        OR dim_order_url_tag.key = 'utm_campaign' AND (dim_order_url_tag.value in ('l-de_n-off_k-free-strap-v1', 'other campaings UTM'))
    ),

    campaign_orders_by_utm_grouped as (
        select
            *
        from
           campaign_orders_by_utm
        group by
            1,2
    ),

    -- ################################
    -- identifying orders_id made with campaign discount code
    -- ################################
    
	-- next line dbt only
	--    
    -- implement a dictionary for mapping the campaigns

    campaign_orders_by_code as (
        select
            order_id,
            -- todo we have implement a dictionary here
            'l-de_n-off_k-free-strap-v1' as campaign_name
        from
        -- TODO replace with a source
            shopify_global.dim_order_discount_code
	-- next two lines only DBT
	--        where
	--            code like any (array['%TRIBE%', '%other campaings codes%'])
	-- next two lines no DBT
        WHERE 
        	dim_order_discount_code.code like '%TRIBE%' or dim_order_discount_code.code like '%other campaings codes%'
        group by
            1,2
    ),

    -- ################################
    -- unioning the CTEs above get all orders from the campaign
    -- ################################
    campaign_orders  as (
        select * from campaign_orders_by_utm_grouped
        union all
        select * from campaign_orders_by_code
    ),

    campaign_orders_grouped as (
        select * from campaign_orders group by 1,2
    ),


    -- ################################
    -- gathering ALL orders enriching it with campaign flag (code & utm flags)
    -- ################################
   final as (
        select
           -- bringing all the columns from the order_enriched
           order_enriched.*,
           -- flagging the campaign orders
           case when campaign_orders_grouped.order_id is not NULL then TRUE else FALSE end as campaign_purchase,
           campaign_orders_grouped.campaign_name,
		-- next two lines added in v02
           case when campaign_orders_by_code.order_id is not null then true else false end as campaign_with_code,
           case when campaign_orders_by_utm_grouped.order_id is not null then true else false end as campaign_with_utm
        -- TODO replace with a source
		-- next line DBT only
		--        from "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
		-- next two lines no DBT        
        from
        	shopify_global.fct_order_enriched order_enriched
        LEFT JOIN 
        	campaign_orders_grouped on order_enriched.id = campaign_orders_grouped.order_id
		-- next two joins added in v02
        left join
			campaign_orders_by_code on order_enriched.id = campaign_orders_by_code.order_id
        left join
			campaign_orders_by_utm_grouped on order_enriched.id = campaign_orders_by_utm_grouped.order_id
		where 
        	order_enriched.financial_status IN
               ('paid', 'partially_refunded')
        )


select
    *
from
    final