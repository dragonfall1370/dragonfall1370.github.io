 

SELECT p.id,
    p.month,
    p.type,
    p.amount,
    max(p._fivetran_synced) AS last_update,
    p.uid
   FROM finance.planning p
  GROUP BY p.id, p.month, p.type, p.amount, p.uid