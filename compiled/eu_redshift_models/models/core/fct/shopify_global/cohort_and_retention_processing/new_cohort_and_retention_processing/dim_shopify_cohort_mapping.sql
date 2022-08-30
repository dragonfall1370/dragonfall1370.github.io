---######################################
---### cohort spine using variable from dbt_project.yml
---#####################################




with 

    cohort_spine_austria as 
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
    where generated_number <= 29
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2020-04-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),
        

    cohort_spine_belgium as 
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
    where generated_number <= 22
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2020-11-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),

    cohort_spine_france as 
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
    where generated_number <= 24
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2020-09-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),

    cohort_spine_germany as 
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
    where generated_number <= 29
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2020-04-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),

    cohort_spine_italy as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 12
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2021-09-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),

    cohort_spine_netherlands as 
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
    where generated_number <= 22
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2020-11-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),

     cohort_spine_sweden as 
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
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
    
    

    )

    select *
    from unioned
    where generated_number <= 6
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2022-03-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),   

     cohort_spine_switzerland as 
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
    where generated_number <= 18
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2021-03-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),    
        
     cohort_spine_unitedkingdom as 
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
    
    

    )

    select *
    from unioned
    where generated_number <= 15
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2021-06-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),           

     cohort_spine_unitedstates as 
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
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
    
    

    )

    select *
    from unioned
    where generated_number <= 3
    order by generated_number



),

all_periods as (

    select (
        

    dateadd(
        month,
        row_number() over (order by 1) - 1,
        '2022-06-01'
        )


    ) as date_month
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_month <= cast(current_date + interval '1 month' as date)

)

select * from filtered


        ),                
     

    union_all as 
        (
            select 
                date_month as cohort,
                'AT' as country 
            from cohort_spine_austria
            union all
            select 
                date_month as cohort,
                'BE' as country 
            from cohort_spine_belgium
            union all
            select 
                date_month as cohort,
                'FR' as country 
            from cohort_spine_france        
            union all
            select 
                date_month as cohort,
                'DE' as country 
            from cohort_spine_germany    
            union all
            select 
                date_month as cohort,
                'IT' as country 
            from cohort_spine_italy    
            union all
            select 
                date_month as cohort,
                'NL' as country 
            from cohort_spine_netherlands 
            union all
            select 
                date_month as cohort,
                'SE' as country 
            from cohort_spine_sweden
            union all
            select 
                date_month as cohort,
                'CH' as country 
            from cohort_spine_switzerland
            union all
            select 
                date_month as cohort,
                'UK' as country 
            from cohort_spine_unitedkingdom
            union all
            select 
                date_month as cohort,
                'US' as country 
            from cohort_spine_unitedstates
        )

select
    country,
    date(cohort) as cohort
from
   union_all