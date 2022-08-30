---created_by: Nham Dao
---###################################################################################################################
        -- this view contains the ticket information for kustomer schema (it cleans up the data from the original conversation table and remove the deleted tickets)
---###################################################################################################################


with amazon_clean as (
    select
    'DE Amazon' as platform
    , amazon.year
    , amazon.week_num
    , amazon.mean
    , min(date_tb.full_date) as full_date
    from "airup_eu_dwh"."web_data"."reviews_de_amazon" amazon 
    left join "airup_eu_dwh"."reports"."dates" date_tb
    on amazon.year = date_tb.year_number 
    and amazon.week_num = date_tb.year_week_number
    where week_num <> 0
    group by 1,2,3,4
    order by full_date desc
    limit 2),
google_clean as (
    select
    'Google Business' as platform
    , google.year
    , google.week_num
    , google.mean
    , min(date_tb.full_date) as full_date
    from "airup_eu_dwh"."web_data"."reviews_google" google  
    left join "airup_eu_dwh"."reports"."dates" date_tb
    on google.year = date_tb.year_number 
    and google.week_num = date_tb.year_week_number
    where week_num <> 0
    group by 1,2,3,4
    order by full_date desc
    limit 2),
io_clean as (
    select
    'Reviews io' as platform
    , review_io.year
    , review_io.week_num
    , review_io.mean
    , min(date_tb.full_date) as full_date
    from "airup_eu_dwh"."web_data"."reviews_io" review_io  
    left join "airup_eu_dwh"."reports"."dates" date_tb
    on review_io.year = date_tb.year_number 
    and review_io.week_num = date_tb.year_week_number
    where week_num <> 0
    group by 1,2,3,4
    order by full_date desc
    limit 2),
trustpilot_clean as (
    select
    'Trust Pilot' as platform
    , review_trustpilot.year
    , review_trustpilot.week_num
    , review_trustpilot.mean
    , min(date_tb.full_date) as full_date
    from "airup_eu_dwh"."web_data"."reviews_trustpilot" review_trustpilot  
    left join "airup_eu_dwh"."reports"."dates" date_tb
    on review_trustpilot.year = date_tb.year_number 
    and review_trustpilot.week_num = date_tb.year_week_number
    where week_num <> 0
    group by 1,2,3,4
    order by full_date desc
    limit 2),
review_union as 
(select *, row_number() over (order by full_date desc) as index 
from amazon_clean
union 
select *, row_number() over (order by full_date desc) as index 
from google_clean
union 
select *, row_number() over (order by full_date desc) as index 
from io_clean
union 
select *, row_number() over (order by full_date desc) as index 
from trustpilot_clean)
select review_union.*, overall.review_star, overall.nr_reviews, overall.webscrape_date from review_union
left join "airup_eu_dwh"."web_data"."reviews_overall" as overall 
on overall.platform = review_union.platform