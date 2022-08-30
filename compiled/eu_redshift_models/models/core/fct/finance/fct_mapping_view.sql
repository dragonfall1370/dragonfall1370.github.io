 

SELECT m.p_l_line,
    m.country,
    m.product,
    m.sub_category,
    m.channel,
    m.cost_category,
    m.cost_center,
    m.id,
    m.region,
    m.account,
    max(m._fivetran_synced) AS last_update,
    m.cost_center_name,
    m.account_name
   FROM "airup_eu_dwh"."finance"."mapping"
  GROUP BY m.p_l_line, m.country, m.product, m.sub_category, m.channel, m.cost_category, m.cost_center, m.id, m.region, m.account