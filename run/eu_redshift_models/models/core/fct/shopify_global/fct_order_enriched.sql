

  create view "airup_eu_dwh"."shopify_global"."fct_order_enriched__dbt_tmp" as (
    ---Authors: Etoma Egot
---Last Modified by: Tomas Kristof

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
        "airup_eu_dwh"."shopify_global"."fct_order_adjustment" foa
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
        "airup_eu_dwh"."shopify_global"."fct_order_line" fol

	left join "airup_eu_dwh"."shopify_global"."fct_order_line_refund" folr
   
		on fol.id = folr.order_line_id
	group by
		order_id),

-- #######################################################
-- Adding the created_at without timezone, and convert created_at to local time without timezone
-- Modified: Long
-- #######################################################


premain as (
select "order_pre".*
        ,case when tz.timezone_citiname is not null then convert_timezone(tz.timezone_citiname, "order_pre".created_at::timestamp)
              when tz.timezone_shortname is not null then convert_timezone(tz.timezone_shortname, "order_pre".created_at::timestamp)
            else convert_timezone('CET', "order_pre".created_at::timestamp)
        end as created_at_premain
        ,case when tz.timezone_citiname is not null then convert_timezone(tz.timezone_citiname, "order_pre".updated_at::timestamp)
              when tz.timezone_shortname is not null then convert_timezone(tz.timezone_shortname, "order_pre".updated_at::timestamp)
            else convert_timezone('CET', "order_pre".updated_at::timestamp)
        end as updated_at_premain
from "airup_eu_dwh"."shopify_global"."fct_order" "order_pre"
left join "airup_eu_dwh"."public"."timezone_configuration" tz on "order_pre".shopify_shop = tz.webshop_name

),
-- #######################################################
-- MAIN QUERY
-- #######################################################

main as (
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
        "order".created_at as created_at_w_tz,
        "order".created_at_premain as created_at,
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
        "order".updated_at as updated_at_w_tz,
        "order".updated_at_premain as updated_at,
        "order".user_id,
        "order".shopify_shop,
        "order".currency_abbreviation,
        "order".conversion_rate_eur,

        -- ###########################
        -- COUNTRY MAPPING INFORMATION
        -- ###########################
        
        coalesce(csam.country_fullname, 'other') as country_fullname,
        coalesce(csam.country_abbreviation, 'other') as country_abbreviation,
        coalesce(csam.country_grouping, 'other') as country_grouping,
        
        -- ######################
        -- CALCULATION OF METRICS
        -- ######################
        
        -- GROSS REVENUE
        "order".total_price as gross_revenue,
        
        -- NET REVENUE 1 : Business logic
        -- "Gross rev (incl. shipping revenue) - VAT - return shipments"
        -- Refunds are calculated via order_adjustments_per_order_id
        case
            when "order".financial_status in ('paid', 'partially_refunded') -- we consider only paid and partial refunds when cmputing net revenue 1
            then "order".total_price + coalesce("order".total_discounts) - coalesce("order".total_tax, 0) - coalesce(order_adjustments_per_order_id.adjustment_amount, 0)
        end as net_revenue_1,
        
        -- calculating net_revenue_1 in USD using odoo daily rates but when not available we use anual average  
        (net_revenue_1
            * case
                when (currency_rates_usd.conversion_rate_eur is null) and (date_trunc('year', updated_at_premain) = '2019-01-01') then 1.120129
                when (currency_rates_usd.conversion_rate_eur is null) and (date_trunc('year', updated_at_premain) = '2020-01-01') then 1.142203
                when (currency_rates_usd.conversion_rate_eur is null) and (date_trunc('year', updated_at_premain) = '2021-01-01') then 1.18318
                else currency_rates_usd.conversion_rate_eur end
        ) as net_revenue_1_usd,
        
        -- NET REVENUE 2: Business logic
        -- "Net revenue 1 minus voucher reduction"
        case
            when "order".financial_status in ('paid', 'partially_refunded') -- we consider only paid and partial refunds when cmputing net revenue 2
            then "order".total_price - coalesce("order".total_tax, 0) - coalesce(order_adjustments_per_order_id.adjustment_amount, 0)
        end as net_revenue_2,
        
        -- calculating net_revenue_2 in USD using odoo daily rates but when not available we use anual average  
        (net_revenue_2
            * case
                when (currency_rates_usd.conversion_rate_eur is null) and (date_trunc('year', updated_at_premain) = '2019-01-01') then 1.120129
                when (currency_rates_usd.conversion_rate_eur is null) and (date_trunc('year', updated_at_premain) = '2020-01-01') then 1.142203
                when (currency_rates_usd.conversion_rate_eur is null) and (date_trunc('year', updated_at_premain) = '2021-01-01') then 1.18318
                else currency_rates_usd.conversion_rate_eur end
        ) as net_revenue_2_usd,
        
        -- NET ORDERS
        -- only orders which have been paid in full or at least partially are to be counted as net orders
        case when "order".financial_status in ('paid', 'partially_refunded') then 1 end as net_orders,
        
        -- GROSS ORDERS
        -- every order should be counted as a gross order
        1 as gross_orders,
        
        -- NET VOLUME
        -- # of Starter Sets/ Pods/ Accessories sold = gross quantity - returned quantity
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
                when lower("order".billing_address_country) = 'united states' THEN 'US'::text
                else 'others'
            end as shop_country
    from
        premain "order"
    left join "airup_eu_dwh"."public"."country_system_account_mapping" csam ON "order".shipping_address_country = (csam.shopify_shipping_address_country)
    ---public.country_system_account_mapping on
        ---"order".shipping_address_country::varchar = any(country_system_account_mapping.shopify_shipping_address_country)
    left join order_adjustments_per_order_id on
        "order".id = order_adjustments_per_order_id.order_id
    left join total_quantity_per_order on
        "order".id = total_quantity_per_order.order_id
	left join odoo_currency.dim_global_currency_rates currency_rates_usd
		on date("order".updated_at_premain) = currency_rates_usd.creation_date
		and currency_rates_usd.currency_abbreviation = 'USD'
    )

select 
    *
from 
    main
  ) with no schema binding;
