

with total_transactions as (
    select order_date, shipping_country, orders as transactions
    from "airup_eu_dwh"."exit_survey"."exit_survey_aggregated_data" 
    where (shipping_country = 'France' or shipping_country = 'United Kingdom')
    and order_date >= '2022-04-13'
    and answer = 'total'
), responses as (
    select order_date, shipping_country,
        (SUM(case when answer = 'total' then orders 
            ELSE NULL::bigint
            end) - 
        SUM(case when answer = 'no response' then orders
            ELSE NULL::bigint
            end)) as responses        
    from "airup_eu_dwh"."exit_survey"."exit_survey_aggregated_data" 
    where (shipping_country = 'France' or shipping_country = 'United Kingdom')
    and order_date >= '2022-04-13'
    group by 1,2 
)    
    select tt.order_date, tt.shipping_country, tt.transactions, r.responses 
    from total_transactions tt
    left join responses r 
    on tt.order_date = r.order_date 
    and tt.shipping_country = r.shipping_country
    order by tt.order_date desc