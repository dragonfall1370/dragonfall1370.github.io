---Authors: Etoma Egot
---Last Modified by: Long VU

---###################################################################################################################

        ---This query adds country mapping information to shopify_global."orders" as well as precalculations for
        ---This is the main enriched order model which combines data from all D2C webshops in a GDPR compliant manner---
        -- This model has been enriched with additionally computed metrics and dimensions such as:
        -- 1. gross revenue : "order".total_price as gross_revenue. Note: total price (includes total discounts)
        -- 2. net revenue 1 : "order".total_price + total discounts - total_tax - return shipments(alias adjustment_amounts)"
        -- 3. net revenue 2 : "order".total_price - total_tax - return shipments(alias adjustment_amounts)"
        -- 4. net volume :  # of Starter Sets/ Pods/ Accessories sold = gross quantity - returned quantity
        -- 5. net orders : only orders which have been paid in full or at least partially are to be counted as net orders
        -- 6: gross orders : every order should be counted as a gross order
        -- 7. shop_country : billing address country
        -- 8. shopify_shop : contains an abbreviation/unique name that describes the webshop where transaction originates
        -- 9: customer_id  : hashed token id of the customer email
        
    ---Note: This model can be used by all users who need enriched order data
---###################################################################################################################

 


with

-- ########################################
-- CALCULATE ORDER ADJUSTMENTS PER ORDER ID
-- ########################################

-- Please note: As of 2021-03-09, there are some open questions around refunds, see https://app.asana.com/0/1160261654509010/1200004660394237/f

order_adjustments_per_order_id as
    (select
        foa.order_id,
        sum(foa.amount) as adjustment_amount
    
    from
        "airup_eu_dwh"."shopify_global_live_con"."fct_order_adjustment_live_con" foa
    group by
        foa.order_id),


-- ########################################
-- CALCULATE THE VOLUME PER ORDER ID
-- ########################################
        
total_quantity_per_order as
    (select
        order_id,
        -- correcting for refunded quantity
        sum(fol.quantity - coalesce(folr.quantity, 0)) as order_total_volume
    from
        "airup_eu_dwh"."shopify_global_live_con"."fct_order_line_live_con" fol

    left join "airup_eu_dwh"."shopify_global_live_con"."fct_order_line_refund_live_con" folr
   
        on fol.id = folr.order_line_id
    group by
        order_id)
-- #######################################################
-- MAIN QUERY
-- #######################################################
,fct_order_enriched_live_con as (
select
    "order".id,
    "order"._fivetran_synced,
    "order".creation_date,
    "order".app_id,
    "order".billing_address_country,
    "order".billing_address_country_code,
    "order".buyer_accepts_marketing,
    "order".cancel_reason,
    "order".cancelled_at,
    "order".checkout_token,
    "order".closed_at,
    "order".confirmed,
    "order".created_at,
    "order".currency,
    "order".shop_cust_id,
    "order".customer_id,
    "order".customer_locale,
    "order".financial_status,
    "order".fulfillment_status,
    "order".landing_site_base_url,
    "order".order_number,
    "order".note,
    "order".number,
    "order".original_order_number,
    "order".payment_gateway_names,
    "order".presentment_currency,
    "order".processed_at,
    "order".processing_method,
    "order".referring_site,
    "order".shipping_address_city,
    "order".shipping_address_country,
    "order".shipping_address_country_code,
    "order".shipping_address_latitude,
    "order".shipping_address_longitude,
    "order".shipping_address_zip,
    "order".source_name,
    "order".subtotal_price,
    "order".subtotal_price_chf,
    "order".subtotal_price_gbp,
    "order".subtotal_price_set,
    "order".taxes_included,
    "order".test,
    "order"."token",
    "order".total_discounts,
    "order".total_discounts_chf,
    "order".total_discounts_gbp,
    "order".total_discounts_set,
    "order".total_line_items_price,
    "order".total_line_items_price_chf,
    "order".total_line_items_price_gbp,
    "order".total_line_items_price_set,
    "order".total_price,
    "order".total_price_chf,
    "order".total_price_gbp,
    "order".total_price_set,
    "order".total_price_usd,
    "order".total_shipping_price_set,
    "order".total_tax,
    "order".total_tax_chf,
    "order".total_tax_gbp,
    "order".total_tax_set,
    "order".total_tip_received,
    "order".total_tip_received_chf,
    "order".total_tip_received_gbp,
    "order".total_weight,
    "order".updated_at,
    "order".user_id,
    "order".shopify_shop,
    "order".currency_abbreviation,
    "order".conversion_rate_eur,
    coalesce(csam.country_fullname, 'other') as country_fullname,
    coalesce(csam.country_abbreviation, 'other') as country_abbreviation,
    coalesce(csam.country_grouping, 'other') as country_grouping,
    "order".total_price as gross_revenue,
    case
        when "order".financial_status in ('paid', 'partially_refunded') -- we consider only paid and partial refunds when cmputing net revenue 1
        then "order".total_price + coalesce("order".total_discounts) - coalesce("order".total_tax, 0) - coalesce(order_adjustments_per_order_id.adjustment_amount, 0)
    end as net_revenue_1,
    case
        when "order".financial_status in ('paid', 'partially_refunded') -- we consider only paid and partial refunds when cmputing net revenue 2
        then "order".total_price - coalesce("order".total_tax, 0) - coalesce(order_adjustments_per_order_id.adjustment_amount, 0)
    end as net_revenue_2,
    case when "order".financial_status in ('paid', 'partially_refunded') then 1 end as net_orders,
    1 as gross_orders,
    case when "order".financial_status in ('paid', 'partially_refunded') then total_quantity_per_order.order_total_volume end as net_volume,
    case
            when lower("order".billing_address_country) = 'netherlands' THEN 'NL'
            when lower("order".billing_address_country) = 'belgium' THEN 'BE'
            when lower("order".billing_address_country) = 'france' THEN 'FR'
            when lower("order".billing_address_country) = 'switzerland' THEN 'CH'
            when lower("order".billing_address_country) = 'united kingdom' THEN 'UK'
            when lower("order".billing_address_country) = 'italy' THEN 'IT'
            when lower("order".billing_address_country) = 'austria' THEN 'AT'
            when lower("order".billing_address_country) = 'germany' THEN 'DE'
            when lower("order".billing_address_country) = 'sweden' THEN 'SE'
            else 'others'
        end as shop_country
from
    "airup_eu_dwh"."shopify_global_live_con"."fct_order_live_con" "order"
left join "airup_eu_dwh"."public"."country_system_account_mapping" csam ON "order".shipping_address_country = (csam.shopify_shipping_address_country)
left join order_adjustments_per_order_id on
    "order".id = order_adjustments_per_order_id.order_id
left join total_quantity_per_order on
    "order".id = total_quantity_per_order.order_id
),
gross_qty_cal as (
select
    order_line.order_id,
    sum(order_line.quantity) as ordered_quantity
from
    "airup_eu_dwh"."shopify_global_live_con"."fct_order_line_live_con" order_line
group by
    order_line.order_id
        )
, fct_order_enriched_live_con_prefn as(
 select     
    case
        when (order_enriched_tmp.created_at - min(order_enriched_tmp.created_at) over (partition by order_enriched_tmp.customer_id,
        order_enriched_tmp.country_fullname)) = '00:00:00'::interval then 'New Customer'::text
        else 'Returning Customer'::text
    end as customer_type
    ,case when order_enriched_tmp.shopify_shop = 'UK' 
        then 
            to_char(convert_timezone('BST', order_enriched_tmp.created_at::timestamp), 'yyyy-mm-dd')
        else
            to_char(convert_timezone('CEST', order_enriched_tmp.created_at::timestamp), 'yyyy-mm-dd') 
    end as order_date
    ,case when order_enriched_tmp.shopify_shop = 'Base' then 'DE' else order_enriched_tmp.shopify_shop end as shopify_shop_2
    ,*
    from fct_order_enriched_live_con order_enriched_tmp
    where
	(order_enriched_tmp.financial_status in ('paid', 'partially_refunded')
	and order_enriched_tmp.customer_id is not null)
)
select order_enriched.customer_type
    ,order_enriched.order_date
    ,order_enriched.country_fullname as country
    ,order_enriched.country_grouping as region
    ,order_enriched.shopify_shop_2 as shopify_shop
    ,sum(gross_orders) as orders
    ,sum(order_enriched.gross_revenue) as gross_revenue
    ,sum(order_enriched.net_revenue_1) as net_revenue_1
    ,sum(order_enriched.net_revenue_2) as net_revenue_2
    ,sum(order_enriched.net_volume) as net_quantity
    ,count(distinct order_enriched.customer_id) as customers
    ,sum(order_shipping_line.price) as gross_shipping_revenue
    ,sum(order_shipping_line.price - coalesce(order_shipping_tax_line.price, 0::double precision)) as net_shipping_revenue
    ,sum(gqc.ordered_quantity) as ordered_quantity
from fct_order_enriched_live_con_prefn order_enriched
left join "airup_eu_dwh"."shopify_global_live_con"."fct_order_shipping_line_live_con" order_shipping_line on
    order_enriched.id = order_shipping_line.order_id
left join "airup_eu_dwh"."shopify_global_live_con"."fct_order_shipping_tax_line_live_con" order_shipping_tax_line on
    order_shipping_line.id = order_shipping_tax_line.order_shipping_line_id
left join gross_qty_cal gqc on gqc.order_id = order_enriched.id
group by 1,2,3,4,5