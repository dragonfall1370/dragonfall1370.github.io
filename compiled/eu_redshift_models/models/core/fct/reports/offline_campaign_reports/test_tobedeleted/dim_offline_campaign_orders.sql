

WITH
    -- ################################
    -- identifying orders_id with campaign UTM parameters
    -- ################################
    campaign_orders_by_utm as (
        select
            dim_order_url_tag.order_id,
            max(
                case when dim_order_url_tag.key = 'utm_campaign' then dim_order_url_tag.value end
                    ) over (partition by dim_order_url_tag.order_id)
            as campaign_name
        from
            shopify_global.dim_order_url_tag
        where 
            (dim_order_url_tag.key = 'utm_source' AND (dim_order_url_tag.value in ('retail')) 
 	        OR dim_order_url_tag.key = 'utm_medium' AND (dim_order_url_tag.value in ('flyer')) 
	        OR dim_order_url_tag.key = 'utm_campaign' AND (dim_order_url_tag.value in ('l-de_n-off_k-free-strap-v1', 'l-de_n-off_k-free-pod-v2'))) -- add new campaings UTM here
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

    campaign_orders_by_code as (
        select
            order_id,
            case
                when code like '%TRIBE%' then 'l-de_n-off_k-free-strap-v1'
                when code like '%FUERDICH%' then 'l-de_n-off_k-free-pod-v2'
            end as campaign_name
        from
        -- TODO replace with a source
            shopify_global.dim_order_discount_code
        WHERE 
        	dim_order_discount_code.code like '%TRIBE%' 
            or dim_order_discount_code.code like '%FUERDICH-%'
        group by
            1,2
    ), 

    -- ################################
    -- unioning the CTEs above get all orders from the campaign
    -- ################################
    campaign_orders  as (
        select * from campaign_orders_by_utm_grouped
        union 
        select * from campaign_orders_by_code
    ),

    -- ################################
    -- gathering ALL orders enriching it with campaign flag (code & utm flags)
    -- ################################
   final as (
        select
           order_id,
           customer_id,
           campaign_name,
           order_enriched.created_at as order_date
        FROM
        	shopify_global.fct_order_enriched order_enriched
        LEFT JOIN 
        	campaign_orders on order_enriched.id = campaign_orders.order_id
        WHERE 
        	campaign_name IS NOT NULL
        )


select
    *
from
    final