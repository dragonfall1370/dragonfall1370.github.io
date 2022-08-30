

with 
    campaign_v1 as (
        with
    campaign_orders_by_utm as (
        select
            dim_order_url_tag.order_id,
            max(
                case when dim_order_url_tag.key = 'utm_campaign' then dim_order_url_tag.value end
                    ) over (partition by dim_order_url_tag.order_id)
            as campaign_name
        from
            "airup_eu_dwh"."shopify_global"."dim_order_url_tag" dim_order_url_tag
        where
            (dim_order_url_tag.key = 'utm_source' AND (dim_order_url_tag.value in ('retail'))
 	        OR dim_order_url_tag.key = 'utm_medium' AND (dim_order_url_tag.value in ('flyer'))
	        OR dim_order_url_tag.key = 'utm_campaign' AND (dim_order_url_tag.value = 'l-de_n-off_k-free-strap-v1')) -- add new campaings UTM here
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
campaign_orders_by_type as (
			select
			dim_orders.campaign_name,
			case when order_enriched.id is not NULL then TRUE else FALSE end as campaign_purchase,
			case when campaign_orders_by_code.order_id is not null then true else false end as campaign_with_code,
            case when campaign_orders_by_utm_grouped.order_id is not null then true else false end as campaign_with_utm,
			order_enriched.id as order_id,
			order_enriched.created_at,
			order_enriched.billing_address_country_code,
			case when campaign_with_code and not campaign_with_utm then 1 else 0 end as order_with_only_campaign_code,
			case when campaign_with_utm and not campaign_with_code then 1 else 0 end as order_with_only_campaign_utm,
			case when campaign_with_code and campaign_with_utm then 1 else 0 end as order_with_both_campaign_code_and_utm
		FROM
				shopify_global.fct_order_enriched order_enriched
				left join dbt_feldm.dim_offline_campaign_orders dim_orders on order_enriched.id = dim_orders.order_id
				left join campaign_orders_by_code on dim_orders.order_id = campaign_orders_by_code.order_id
        		left join campaign_orders_by_utm_grouped on dim_orders.order_id = campaign_orders_by_utm_grouped.order_id
		where
			dim_orders.campaign_name = 'l-de_n-off_k-free-strap-v1' and
			campaign_purchase
			)
select
		date_trunc('day', created_at) as "date",
		billing_address_country_code as country,
		sum(order_with_only_campaign_code) as orders_with_only_campaign_code,
		sum(order_with_only_campaign_utm) as orders_with_only_campaign_utm,
		sum(order_with_both_campaign_code_and_utm) as orders_with_both_campaign_code_and_utm
	from
		campaign_orders_by_type
	group by
		date_trunc('day', created_at),
		billing_address_country_code
    ),

    campaign_v2 as(
        with
    campaign_orders_by_utm as (
        select
            dim_order_url_tag.order_id,
            max(
                case when dim_order_url_tag.key = 'utm_campaign' then dim_order_url_tag.value end
                    ) over (partition by dim_order_url_tag.order_id)
            as campaign_name
        from
            "airup_eu_dwh"."shopify_global"."dim_order_url_tag" dim_order_url_tag
        where
            (dim_order_url_tag.key = 'utm_source' AND (dim_order_url_tag.value in ('retail'))
 	        OR dim_order_url_tag.key = 'utm_medium' AND (dim_order_url_tag.value in ('flyer'))
	        OR dim_order_url_tag.key = 'utm_campaign' AND (dim_order_url_tag.value = 'l-de_n-off_k-free-strap-v2')) -- add new campaings UTM here
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
campaign_orders_by_type as (
			select
			dim_orders.campaign_name,
			case when order_enriched.id is not NULL then TRUE else FALSE end as campaign_purchase,
			case when campaign_orders_by_code.order_id is not null then true else false end as campaign_with_code,
            case when campaign_orders_by_utm_grouped.order_id is not null then true else false end as campaign_with_utm,
			order_enriched.id as order_id,
			order_enriched.created_at,
			order_enriched.billing_address_country_code,
			case when campaign_with_code and not campaign_with_utm then 1 else 0 end as order_with_only_campaign_code,
			case when campaign_with_utm and not campaign_with_code then 1 else 0 end as order_with_only_campaign_utm,
			case when campaign_with_code and campaign_with_utm then 1 else 0 end as order_with_both_campaign_code_and_utm
		FROM
				shopify_global.fct_order_enriched order_enriched
				left join dbt_feldm.dim_offline_campaign_orders dim_orders on order_enriched.id = dim_orders.order_id
				left join campaign_orders_by_code on dim_orders.order_id = campaign_orders_by_code.order_id
        		left join campaign_orders_by_utm_grouped on dim_orders.order_id = campaign_orders_by_utm_grouped.order_id
		where
			dim_orders.campaign_name = 'l-de_n-off_k-free-strap-v2' and
			campaign_purchase
			)
select
		date_trunc('day', created_at) as "date",
		billing_address_country_code as country,
		sum(order_with_only_campaign_code) as orders_with_only_campaign_code,
		sum(order_with_only_campaign_utm) as orders_with_only_campaign_utm,
		sum(order_with_both_campaign_code_and_utm) as orders_with_both_campaign_code_and_utm
	from
		campaign_orders_by_type
	group by
		date_trunc('day', created_at),
		billing_address_country_code   
    ),

    final as(
        select *, 'l-de_n-off_k-free-strap-v1' as campaign_name from campaign_v1 
        union all
        select *, 'l-de_n-off_k-free-strap-v2' as campaign_name from campaign_v2
    )

select * from final