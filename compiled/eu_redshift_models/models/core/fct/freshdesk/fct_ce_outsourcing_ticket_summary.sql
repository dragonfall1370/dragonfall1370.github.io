---Authors: Nham Dao
---Last Modified by: Nham Dao



--/*#################################################################
--this view prepares data for the outsourcing agent performance
--#################################################################*/
 

with get_first_response as (select ticket_id, contact_id , updated_at , row_number () over (partition by ticket_id order by updated_at) as rn
from "airup_eu_dwh"."freshdesk"."dim_freshdesk_conversation"
where incoming is false 
and private is false),
sla_check as (select a.contact_email, a.contact_name , gfr.ticket_id, dft.created_at ,gfr.updated_at as first_respond_time, dft.fr_due_by,
          CASE
              WHEN gfr.updated_at <= fr_due_by THEN TRUE
              ELSE FALSE
          END AS first_respond_within_SLA, 
             datediff(minute, dft.created_at::TIMESTAMP, gfr.updated_at::TIMESTAMP) AS minute_dif
from freshdesk.agent a 
left join get_first_response gfr
on rn = 1
and gfr.contact_id = a.id
left join "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" dft 
on dft.id = gfr.ticket_id
WHERE a.contact_email like '%partner%'),
fcr_ticket AS
  (SELECT sum(CASE
                  WHEN incoming = TRUE 
                  AND from_email is not null  
                  THEN 1
                  ELSE 0
              END) AS number_of_reply,
          ticket_id
   FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_conversation"
   GROUP BY ticket_id),
ticket_resolved as (SELECT a.contact_email,
          a.contact_name,
          dft.id AS ticket_id,
          date(dft.created_at) AS created_at,
          responder_id,
          date(stats_resolved_at) AS stats_resolved_at,
          status
   FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" AS dft
   LEFT JOIN "airup_eu_dwh"."freshdesk"."agent" AS a ON dft.responder_id = a.id
   WHERE a.contact_email like '%partner%'
     AND spam = FALSE
     AND deleted IS NULL),
     union_ticket_table as (
select contact_email,
                    contact_name,
                    ticket_id,
                    status, 
                    'stats_resolved_at' AS type_of_date,
                    stats_resolved_at AS date_value,
                    null as first_respond_within_SLA,
                    null as minute_dif
   FROM ticket_resolved
union all
select contact_email,
                    contact_name,
                    ticket_id,
                    null as status, 
                    'first_respond_at' AS type_of_date,
                    date(first_respond_time) AS date_value,
                    first_respond_within_SLA,
                    minute_dif
from sla_check), 
date_spine as (
        select full_date  as "date" from reports.dates
        where full_date >='2020-01-01'
        and full_date <= current_date 
    )
SELECT dd.date as date_actual,
       ut.contact_email,
       ut.contact_name,
       ut.ticket_id,
       ut.type_of_date,
       ut.first_respond_within_SLA,
       ut.minute_dif,
       ut.status,
       ft.number_of_reply
FROM date_spine AS dd
LEFT JOIN union_ticket_table AS ut ON dd.date = ut.date_value
LEFT JOIN fcr_ticket ft ON ut.ticket_id = ft.ticket_id