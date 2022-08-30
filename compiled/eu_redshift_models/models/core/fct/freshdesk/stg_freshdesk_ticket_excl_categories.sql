--created by: Nham Dao
--this view use dim_freshdes_ticket_excl_some_categories as the data source, we excluded some categories which should not be used to calculate contact rate

 
SELECT id AS ticket_id,
    created_at::date AS creation_date,
        CASE
            WHEN priority = 1 THEN 'Low'::text
            WHEN priority = 2 THEN 'Medium'::text
            WHEN priority = 3 THEN 'High'::text
            WHEN priority = 4 THEN 'Medium'::text
            ELSE 'others'::text
        END AS priority,
        CASE
            WHEN source = 1 THEN 'Email'::text
            WHEN source = 2 THEN 'Portal'::text
            WHEN source = 3 THEN 'Phone'::text
            WHEN source = 7 THEN 'Chat'::text
            WHEN source = 9 THEN 'Feedback widget'::text
            WHEN source = 10 THEN 'Outbound email'::text
            ELSE 'others'::text
        END AS source,
        CASE
            WHEN status = 2 THEN 'Open'::text
            WHEN status = 3 THEN 'Pending'::text
            WHEN status = 4 THEN 'Resolved'::text
            WHEN status = 5 THEN 'Closed'::text
            WHEN status = 6 THEN 'Waiting on customer'::text
            WHEN status = 7 THEN 'Waiting on third party'::text
            ELSE 'others'::text
        END AS status,
        CASE
            WHEN custom_cf_category IS NULL THEN 'None'
            ELSE custom_cf_category
        END AS issue_category,
        CASE
            WHEN custom_cf_subcategory_1 IS NULL THEN 'None'
            ELSE custom_cf_subcategory_1
        END AS issue_subcategory1,
        CASE
            WHEN custom_cf_subcategory_2 IS NULL THEN 'None'
            ELSE custom_cf_subcategory_2
        END AS issue_subcategory2,
    custom_cf_kundensprache AS customer_language,
        CASE
            WHEN custom_cf_country IS NULL AND custom_cf_kundensprache = 'Français'::text THEN 'France'
            WHEN custom_cf_country IS NULL AND (custom_cf_kundensprache in  ('Dutch', 'Nederlands')) THEN 'Netherlands'
            WHEN custom_cf_country IS NULL AND custom_cf_kundensprache = 'Italiano'::text THEN 'Italy'
            ELSE custom_cf_country
        END AS country,
        CASE
           WHEN dim_freshdesk_ticket.custom_cf_productcategory IS NULL THEN 'Unspecified'::character varying
           ELSE dim_freshdesk_ticket.custom_cf_productcategory
           END                                      AS product_category,
        CASE
           WHEN dim_freshdesk_ticket.custom_cf_product IS NULL THEN 'Unspecified'::character varying
           ELSE dim_freshdesk_ticket.custom_cf_product
           END                                      AS product,
        CASE
           WHEN dim_freshdesk_ticket.custom_cf_product_characteristic IS NULL THEN 'Unspecified'::character varying
           ELSE dim_freshdesk_ticket.custom_cf_product_characteristic
           END                                      AS product_characteristics
   FROM "airup_eu_dwh"."freshdesk"."dim_freshdes_ticket_excl_some_categories" dim_freshdesk_ticket
  WHERE spam = false AND deleted IS NULL AND created_at::date > '2020-09-30'::date