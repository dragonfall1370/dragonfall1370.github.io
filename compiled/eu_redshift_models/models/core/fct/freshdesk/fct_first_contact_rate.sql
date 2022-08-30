---Authors: Nham Dao
---Last Modified by: Nham Dao

---###################################################################################################################

        --- This view contains data to calculate first contact rate
---###################################################################################################################

 

WITH reply_count AS (
         SELECT sum(
                CASE
                    WHEN dfc.incoming = true AND dfc.from_email IS NOT NULL THEN 1
                    ELSE 0
                END) AS number_of_cus_contact,
            dfc.ticket_id
           FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_conversation" dfc
          GROUP BY dfc.ticket_id
        )
 SELECT date(dft.stats_resolved_at) AS resolved_date,
    dft.status,
    dft.id,
    rc.number_of_cus_contact,
    rc.ticket_id
   FROM "airup_eu_dwh"."freshdesk"."dim_freshdesk_ticket" dft
     LEFT JOIN reply_count rc ON dft.id = rc.ticket_id