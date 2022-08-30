

SELECT pa1."day",pa1.person_id, pa1.flow_id, pa1.flow_name, pa1.flow_message_id, pa1.flow_message_name, pa1.status, sum(pa1.delivered) AS delivered, count(
        CASE
            WHEN pa1.opens > 0 THEN pa1.opens
            ELSE NULL
        END) AS unique_opens, count(
        CASE
            WHEN pa1.clicks > 0 THEN pa1.clicks
            ELSE NULL
        END) AS unique_clicks, sum(pa1.placed_order) AS placed_order, sum(pa1.revenue) AS revenue, sum(pa1.unsubscribes) AS unsubcribes, sum(pa1.bounces) AS bounces
   FROM ( SELECT date_trunc('day', e."datetime")::date AS "day", flow.id AS flow_id, flow.name AS flow_name, e.flow_message_id, e.property_campaign_name AS flow_message_name, flow.status, e.person_id, sum(
                CASE
                    WHEN e."type" = 'Received Email' OR e."type" = 'Received Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS delivered, sum(
                CASE
                    WHEN e."type" = 'Opened Email' OR e."type" = 'Opened Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS opens, sum(
                CASE
                    WHEN e."type" = 'Clicked Email' OR e."type" = 'Clicked Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS clicks, sum(
                CASE
                    WHEN e."type" = 'Placed Order' THEN 1
                    ELSE 0
                END) AS placed_order, COALESCE(sum(e.property_value), 0::double precision) AS revenue, sum(
                CASE
                    WHEN e."type" = 'Unsubscribed' OR e."type" = 'Unsubscribed from List' THEN 1
                    ELSE 0
                END) AS unsubscribes, sum(
                CASE
                    WHEN e."type" = 'Bounced Email' OR e."type" = 'Bounced Email (MailChimp)' THEN 1
                    ELSE 0
                END) AS bounces
           FROM "airup_eu_dwh"."klaviyo_global"."dim_event" e
      LEFT JOIN "airup_eu_dwh"."klaviyo_global"."dim_flow" flow ON e.flow_id = flow.id
     GROUP BY date_trunc('day', e."datetime")::date, flow.id, flow.name, e.flow_message_id, e.property_campaign_name, flow.status, e.person_id
     ORDER BY date_trunc('day', e."datetime")::date) pa1
  WHERE pa1.flow_id IS NOT NULL AND pa1.flow_name IS NOT NULL AND pa1.flow_message_id IS NOT NULL AND pa1.flow_message_name IS NOT NULL
  GROUP BY pa1."day", pa1.flow_id, pa1.flow_name, pa1.flow_message_id, pa1.flow_message_name, pa1.status,pa1.person_id