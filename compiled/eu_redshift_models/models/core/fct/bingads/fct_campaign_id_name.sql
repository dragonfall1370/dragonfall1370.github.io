

SELECT DISTINCT ch.id AS campaign_id,
    ch.name AS campaign_name
   FROM "airup_eu_dwh"."bingads"."campaign_history" ch