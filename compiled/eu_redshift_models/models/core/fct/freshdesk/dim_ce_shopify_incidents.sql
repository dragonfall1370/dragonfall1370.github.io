--created by: Etoma Egot
 


WITH cte_shopify AS (
SELECT o.created_at::date AS order_date,
            date_trunc('month', o.created_at::date::timestamp with time zone) AS month,
            date_part('year', o.created_at::date) AS year,
            sum(o.gross_orders) AS orders
           FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched"  o
          GROUP BY (o.created_at::date), (to_char(o.created_at::date::timestamp with time zone, 'Mon'::text)), (('Q'::text || ''::text) || date_part('year', o.created_at::date))
        ), cte_incident_before_categorization AS (
         SELECT f.created_at::date AS creation_date,
            count(DISTINCT f.id) AS ticket_count,
            sum(
                CASE
                    WHEN f.status = 5 THEN 1
                    ELSE 0
                END) AS tickets_resolved,
            sum(
                CASE
                    WHEN f.status <> 5 THEN 1
                    ELSE 0
                END) AS tickets_open,
            sum(
                CASE
                    WHEN f.source = 1 THEN 1
                    ELSE 0
                END) AS tickets_by_email,
            sum(
                CASE
                    WHEN f.source = 9 THEN 1
                    ELSE 0
                END) AS tickets_by_widget,
            sum(
                CASE
                    WHEN f.source = 7 THEN 1
                    ELSE 0
                END) AS tickets_by_chat,
            sum(
                CASE
                    WHEN f.source = 2 THEN 1
                    ELSE 0
                END) AS tickets_by_portal
           FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" f
          WHERE date_part('year', f.created_at::date) = 2020::double precision AND date_part('month', f.created_at::date) >= 1::double precision AND date_part('month', f.created_at::date) <= 9::double precision AND f.custom_cf_hauptkategorie IS NOT NULL AND f.spam = false AND f.deleted IS NULL
          GROUP BY (f.created_at::date)
          ORDER BY (f.created_at::date)
        ), cte_incident_after_categorization AS (
         SELECT f.created_at::date AS creation_date,
            count(DISTINCT f.id) AS ticket_count,
            sum(
                CASE
                    WHEN f.status = 5 THEN 1
                    ELSE 0
                END) AS tickets_resolved,
            sum(
                CASE
                    WHEN f.status <> 5 THEN 1
                    ELSE 0
                END) AS tickets_open,
            sum(
                CASE
                    WHEN f.source = 1 THEN 1
                    ELSE 0
                END) AS tickets_by_email,
            sum(
                CASE
                    WHEN f.source = 9 THEN 1
                    ELSE 0
                END) AS tickets_by_widget,
            sum(
                CASE
                    WHEN f.source = 7 THEN 1
                    ELSE 0
                END) AS tickets_by_chat,
            sum(
                CASE
                    WHEN f.source = 2 THEN 1
                    ELSE 0
                END) AS tickets_by_portal
        FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" f
          WHERE date(f.created_at) > '2020-09-30'::date AND f.spam = false AND f.deleted IS NULL
          GROUP BY (f.created_at::date)
          ORDER BY (f.created_at::date)
        ), unionized_incidents AS (
         SELECT cte_incident_before_categorization.ticket_count,
            cte_incident_before_categorization.tickets_resolved,
            cte_incident_before_categorization.tickets_open,
            cte_incident_before_categorization.tickets_by_email,
            cte_incident_before_categorization.tickets_by_widget,
            cte_incident_before_categorization.tickets_by_portal,
            cte_incident_before_categorization.tickets_by_chat,
            cte_incident_before_categorization.creation_date
           FROM cte_incident_before_categorization
        UNION ALL
         SELECT cte_incident_after_categorization.ticket_count,
            cte_incident_after_categorization.tickets_resolved,
            cte_incident_after_categorization.tickets_open,
            cte_incident_after_categorization.tickets_by_email,
            cte_incident_after_categorization.tickets_by_widget,
            cte_incident_after_categorization.tickets_by_portal,
            cte_incident_after_categorization.tickets_by_chat,
            cte_incident_after_categorization.creation_date
           FROM cte_incident_after_categorization
        ), unified_data AS (
         SELECT cte_shopify.order_date,
            cte_shopify.year,
            cte_shopify.month,
            cte_shopify.orders,
            unionized_incidents.ticket_count AS tickets,
            unionized_incidents.tickets_resolved,
            unionized_incidents.tickets_open,
            unionized_incidents.tickets_by_email,
            unionized_incidents.tickets_by_widget,
            unionized_incidents.tickets_by_portal,
            unionized_incidents.tickets_by_chat,
            max(unionized_incidents.ticket_count) OVER (PARTITION BY ((cte_shopify.month || ''::text) || cte_shopify.year) ORDER BY ((cte_shopify.month || ''::text) || cte_shopify.year) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_tickets
           FROM cte_shopify
             JOIN unionized_incidents ON unionized_incidents.creation_date = cte_shopify.order_date
          GROUP BY cte_shopify.order_date, cte_shopify.year, cte_shopify.month, cte_shopify.orders, unionized_incidents.ticket_count, unionized_incidents.tickets_resolved, unionized_incidents.tickets_open, unionized_incidents.tickets_by_email, unionized_incidents.tickets_by_widget, unionized_incidents.tickets_by_portal,unionized_incidents.tickets_by_chat
          ORDER BY cte_shopify.order_date
        )
 SELECT unified_data.order_date,
    unified_data.orders,
    unified_data.tickets,
    unified_data.tickets_resolved,
    unified_data.tickets_open,
    unified_data.tickets_by_email,
    unified_data.tickets_by_widget,
    unified_data.tickets_by_portal,
    unified_data.tickets_by_chat,    
    COALESCE(
        CASE
            WHEN unified_data.order_date = (dateadd('day',-1, (date_trunc('month', ( dateadd('month',1,unified_data.order_date::date))))))::date THEN unified_data.max_tickets::numeric
            ELSE NULL::numeric
        END, 0::numeric) AS max_tickets
   FROM unified_data
  ORDER BY unified_data.order_date DESC