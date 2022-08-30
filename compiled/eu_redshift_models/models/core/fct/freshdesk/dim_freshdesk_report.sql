--created by: Etoma Egot
 

WITH incidents AS (
         SELECT count(ticket_id) AS tickets_created,
            sum(
                CASE
                    WHEN lower(status) = 'closed'::text THEN 1
                    ELSE 0
                END) AS tickets_resolved,
            creation_date,
            priority,
            source,
            status,
            issue_category,
            issue_subcategory1,
            issue_subcategory2,
            customer_language,
            country,
            product_category,
            product,
            product_characteristics
           FROM "airup_eu_dwh"."freshdesk"."stg_freshdesk_ticket"
          GROUP BY 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14
        )
 SELECT sum(tickets_created) AS tickets_created,
    sum(tickets_resolved) AS tickets_resolved,
    creation_date,
    priority,
    source,
    status,
    issue_category,
    issue_subcategory1,
    issue_subcategory2,
    case when customer_language is null then 'Unspecified' else customer_language end as customer_language,
    case when country is null then 'Unspecified'
    when country = 'UK' then 'United Kingdom' else country end as country,
    product_category,
    product,
    product_characteristics
   FROM incidents
  GROUP BY 3, 4, 5, 6, 7, 8, 9, 10,11,12,13,14