---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains freshdesk tickets with dg-public tag
---###################################################################################################################

 

WITH dg_public AS (
         SELECT tt_1."tag",
            tt_1.ticket_id AS public_ticket_id
           FROM "airup_eu_dwh"."freshdesk"."ticket_tag" tt_1
          WHERE tt_1."tag"::text = 'dg-public'::text
        ), ticket_with_tag AS (
         SELECT DISTINCT tt_1.ticket_id AS tag_ticket_id,
            dp."tag",
            dp.public_ticket_id
           FROM "airup_eu_dwh"."freshdesk"."ticket_tag" tt_1
             LEFT JOIN dg_public dp ON tt_1.ticket_id = dp.public_ticket_id
        )
 SELECT date(dft.created_at) AS created_at,
    dft.id,
    dft.custom_cf_dgstatus,
    tt."tag",
    tt.tag_ticket_id,
    tt.public_ticket_id,
    dft.source
   FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" dft
     LEFT JOIN ticket_with_tag tt ON dft.id = tt.tag_ticket_id
  WHERE date(dft.created_at) >= '2020-01-01'::date AND dft.spam = false AND dft.deleted IS NULL