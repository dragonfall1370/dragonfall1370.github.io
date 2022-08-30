

  create  table
    "airup_eu_dwh"."dbt_feldm"."dim_shopify_product_cohort_spine__dbt_tmp"
    
    
    
  as (
    


with

    date_spine as 
        (
            

/*
call as follows:

date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dateadd(week, 1, current_date)"
)

*/

with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 811
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        day,
        row_number() over (order by 1) - 1,
        '2020-04-01'
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast(current_date as date)

)

select * from filtered


        ),


    month_diff_spine as (
        

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
    
    

    )

    select *
    from unioned
    where generated_number <= 25
    order by generated_number


    ),

    country_spine as (
        select distinct 
            country,
            region
        from
            "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_cohort_processing_new"
    ),

    first_ss_bundle_init_order_spine as (
        

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
    
    
    + 1
    as generated_number

    from

    
    p as p0
    
    

    )

    select *
    from unioned
    where generated_number <= 2
    order by generated_number


    ),

    products_spine as (
        select distinct 
            case when category = 'Hardware' then 'Flavour' else category end as category,
            case when subcategory_4 is NULL then subcategory_3 else subcategory_4 end as product
        from
            "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_cohort_processing_new"
        where
            product is not null
    ),

    drinkingsystems_spine as (
        select distinct 
            category,
            subcategory_3 as product
        from
            "airup_eu_dwh"."dbt_feldm"."fct_shopify_product_cohort_processing_new"
        where
            product is not null
            and category = 'Hardware'
    ),

    product_and_ds_spine as (
        select * from products_spine
        union all
        select * from drinkingsystems_spine
    ),

    final as (
        select
            date_spine.date_day::date as date_spine,
            product.product as product_spine,
            product.category as category_spine,
            month.generated_number -1 as month_diff_spine,
            country.country as country_spine,
            country.region as region_spine,
            bundle.generated_number -1 as first_ss_bundle_init_order_spine
        from 
            date_spine 
            left join product_and_ds_spine product on 1=1
            left join month_diff_spine month on 1=1
            left join country_spine country on 1=1
            left join first_ss_bundle_init_order_spine bundle on 1=1
    )

select 
    *
from
    final
  );