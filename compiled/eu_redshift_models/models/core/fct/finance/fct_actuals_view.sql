 

SELECT a.id,
    a.invoice_number,
    a.month,
    a.type,
    a.amount,
    a.description,
    a.uid,
    max(a._fivetran_synced) AS last_updated
   FROM "airup_eu_dwh"."finance"."actuals"
  GROUP BY a.id, a.invoice_number, a.month, a.type, a.amount, a.description, a.uid