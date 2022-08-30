---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains data for outsourcing agent
---###################################################################################################################

 

SELECT a.contact_email,
    a.contact_name,
    date(c.created_at) AS created_at,
    c.updated_at,
    c.contact_id,
    c.private,
    c.id,
    c.ticket_id
   FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_conversation" c
     JOIN "airup_eu_dwh"."freshdesk"."agent" a ON c.contact_id = a.id AND a.contact_email::text ~~ '%partner%'::text
  WHERE c.private = ANY (ARRAY[false, NULL::boolean])