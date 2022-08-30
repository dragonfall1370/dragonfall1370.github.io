

WITH pa1 AS (
    SELECT 
        f.name AS flow_name
        , e.person_id
        , e.shop
        , date_trunc('day', e."datetime") AS send_time
        , e.flow_id
--        , CASE WHEN p.custom_consent IS NOT NULL THEN 'opted_in' ELSE 'not_opted_in' END AS custom_consent
        , SUM(CASE WHEN e.type = 'Placed Order' THEN 1 ELSE 0 END) AS placed_order
--        , COALESCE(SUM(e.property_value), 0) AS revenue
        , SUM(CASE WHEN e.type = 'Opened Email' OR e.type= 'Opened Email (MailChimp)' THEN 1 ELSE 0 END) AS opens
        , SUM(CASE WHEN e.type = 'Clicked Email' OR e.type = 'Clicked Email (MailChimp)' THEN 1 ELSE 0 END) AS clicks
        , SUM(CASE WHEN e.type = 'Unsubscribed' OR e.type = 'Unsubscribed from List' THEN 1 ELSE 0 END) AS unsubscribes
        , SUM(CASE WHEN e.type = 'Received Email' OR e.type = 'Received Email (MailChimp)' THEN 1 ELSE 0 END) AS receieved
        , SUM(CASE WHEN e.type = 'Dropped Email' THEN 1 ELSE 0 END) AS dropped
        , SUM(CASE WHEN e.type = 'Bounced Email' OR e.type = 'Bounced Email (MailChimp)' THEN 1 ELSE 0 END) AS bounces
    FROM "airup_eu_dwh"."klaviyo_global"."dim_event" e
    LEFT JOIN "airup_eu_dwh"."klaviyo_global"."dim_flow" f ON e.flow_id = f.id
    LEFT JOIN "airup_eu_dwh"."klaviyo_global"."dim_person" p ON e.person_id = p.id
    GROUP BY f.name, e.person_id, e.shop, date_trunc('day', e."datetime"), e.flow_id
--    , CASE WHEN p.custom_consent IS NOT NULL THEN 'opted_in' ELSE 'not_opted_in' END
), pa2 AS (
    SELECT 
        pa1.flow_name
        , pa1.shop
--        , pa1.custom_consent
        , pa1.send_time
        , pa1.flow_id
        , SUM(CASE WHEN (pa1.receieved - pa1.dropped) >=1 THEN 1 ELSE 0 END) AS customers
        , SUM(pa1.placed_order) AS placed_order
        , COUNT(CASE WHEN pa1.opens > 0 THEN pa1.opens ELSE NULL END) AS unique_opens
        , COUNT(CASE WHEN pa1.clicks > 0 THEN pa1.clicks ELSE NULL END) AS unique_clicks
        , SUM(pa1.clicks) AS total_clicks
        , SUM(pa1.opens) AS total_opens
        , SUM(pa1.unsubscribes) AS unsubscribes
        , SUM(pa1.receieved) - SUM(pa1.dropped) AS successful_deliveries
        , SUM(pa1.bounces) AS bounces
    FROM pa1
    GROUP BY pa1.flow_name, pa1.shop, pa1.send_time, pa1.flow_id
 --   , pa1.custom_consent
)
SELECT 
    pa2.flow_name
    , pa2.shop
--    , pa2.custom_consent
    , pa2.customers
    , pa2.flow_id
    , pa2.send_time
    , pa2.unique_clicks
    , pa2.total_clicks
    , pa2.unique_opens
    , pa2.total_opens
    , pa2.unsubscribes
    , pa2.successful_deliveries
    , pa2.bounces
    , SUM(fr.order_count) AS placed_order
    , SUM(fr.eur_revenue) AS revenue
FROM pa2
LEFT JOIN "airup_eu_dwh"."klaviyo_global"."fct_flow_revenue_eur" fr ON pa2.flow_id = fr.flow_id AND date_trunc('day', pa2.send_time) = fr."date"
WHERE fr.metric_id IN ('W6xipc','SGUEKE','Tsnt2g','T6pvXU','VubSkw','UQQ8CB','TNKAfF', 'Wdbk5M')
GROUP BY pa2.flow_name, pa2.shop, pa2.customers, pa2.flow_id, pa2.send_time, pa2.unique_clicks, pa2.total_clicks, pa2.unique_opens, pa2.total_opens, pa2.unsubscribes, pa2.successful_deliveries, pa2.bounces
--, pa2.custom_consent