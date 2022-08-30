--created by: Nham Dao
--this view use dim_freshdes_ticket_excl_some_categories as the data source, we excluded some categories which should not be used to calculate contact rate

 

with
    -- creates a date spine (day-steps) between 2020-09-30 and current date
    -- this spine is used for spining in shopify CTEs & freshdesk CTEs
    date_spine as (
        select full_date  as "date" from reports.dates
        where full_date >='2020-09-30'
        and full_date <= current_date 
    ),

--     /*#################################################################
--     the following CTEs are creating the neeeded spines for shopify orders
--     and populates them with shopify data from shopify orders
--     #################################################################*/

    -- creates a country spine and adds unspecified to accommodate for Null/Unspecified countries in the incidents CTE
    order_spine as (
        SELECT DISTINCT
            order_enriched.country_fullname as country
        FROM  "airup_eu_dwh"."shopify_global"."fct_order_enriched" as order_enriched
        UNION
        SELECT 'Unspecified'
    ),

    -- combines a order_spine spine with date_spine
    order_and_date_spine as (
       SELECT
           order_spine.country,
           date_spine.date
       FROM order_spine
           LEFT JOIN date_spine ON 1=1
     ),

    -- gathers shopify orders
    orders as (
        SELECT
               sum(order_enriched.gross_orders)                          AS orders,
               date(date_trunc('day'::text, order_enriched.created_at)) AS date,
               order_enriched.country_fullname AS country
        FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" order_enriched
        GROUP BY 2,3
            ),

    -- populates the order_and_date spine with shopify orders
     orders_spined as (
         SELECT
            order_and_date_spine.date,
            order_and_date_spine.country,
            COALESCE(orders.orders, 0) orders
         FROM
            order_and_date_spine
            LEFT JOIN orders
                ON order_and_date_spine.country = orders.country
                AND order_and_date_spine.date = orders.date
     ),

--     /*#################################################################
--     the following CTEs are creating the neeeded spines for incidents orders
--     and populates them with incident data from freshdesk
--     #################################################################*/

--   creates a spine from all the distinct dimension possibilities
-- test
     incidents_spine as (
         SELECT DISTINCT
            CASE WHEN f.customer_language IS NULL THEN 'Unspecified' ELSE f.customer_language END as customer_language,
            CASE WHEN f.source IS NULL THEN 'Unspecified' ELSE f.source END as source,
            CASE
                WHEN f.country IS NULL THEN 'Unspecified'
                WHEN f.country = 'UK' THEN 'United Kingdom'
                WHEN f.country = 'Others' THEN 'other'
                ELSE f.country
            END as country
         FROM
             "airup_eu_dwh"."freshdesk"."stg_freshdesk_ticket_excl_categories" f

     ),

   -- combines the incident_spine spine with date_spine
     incidents_spine_and_date_spine as (
         SELECT
            date_spine.date,
            incidents_spine.*
         FROM
            date_spine
            LEFT JOIN incidents_spine on 1=1
     ),

    -- gathers incidents data and align inconsistencies in dimensions (e.g. UK -> United Kingdom)
     incidents AS (
         SELECT
            count(distinct f.ticket_id) AS tickets_created,
            date(date_trunc('day', f.creation_date)) AS date,
            CASE WHEN f.source IS NULL THEN 'Unspecified' ELSE f.source END as source,
            CASE WHEN f.customer_language IS NULL THEN 'Unspecified' ELSE f.customer_language END as customer_language,
            CASE
                WHEN f.country IS NULL THEN 'Unspecified'
                WHEN f.country = 'UK' THEN 'United Kingdom'
                WHEN f.country = 'Others' THEN 'other'
                ELSE f.country
            END as country
         FROM
            "airup_eu_dwh"."freshdesk"."stg_freshdesk_ticket_excl_categories" f
    --         { ref ('stg_freshdesk_ticket')}} f
         GROUP BY 2,3,4,5),

    -- populates the incidents_spine_and_date spine with incidents
    incidents_spined as (
        SELECT
            incidents_spine_and_date_spine.*,
            COALESCE(incidents.tickets_created, 0) as tickets_created
        FROM
            incidents_spine_and_date_spine
            LEFT JOIN incidents ON
                incidents_spine_and_date_spine.date = incidents.date
                AND incidents_spine_and_date_spine.source = incidents.source
                AND incidents_spine_and_date_spine.customer_language = incidents.customer_language
                AND incidents_spine_and_date_spine.country = incidents.country
    ),

--     /*#################################################################
--     joining spinnd shopify & incidents orders into a final dataset
--     #################################################################*/

    final as (
        SELECT
            orders_spined.date,
            case when orders_spined.country='other' then 'Others' else orders_spined.country end as country ,
            orders_spined.orders,
            incidents_spined.tickets_created,
            incidents_spined.source,
            incidents_spined.customer_language
        FROM
            orders_spined
            LEFT JOIN incidents_spined
                ON orders_spined.date =  incidents_spined.date
                   AND
                   orders_spined.country =  incidents_spined.country

    )

select
*
from
    final