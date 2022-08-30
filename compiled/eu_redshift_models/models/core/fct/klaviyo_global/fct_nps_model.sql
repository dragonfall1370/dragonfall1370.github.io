

WITH shopify_dimensions AS (
    SELECT 
        "order".email,
        "order".id,
        "order".created_at::date,
        "order".total_price,
        ROW_NUMBER() OVER (PARTITION BY "order".email ORDER BY "order".created_at) AS order_number,
        LISTAGG (order_line.title, ', ') AS basket,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.category = 'Hardware' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_bottle,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_1 = 'Starter Set' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_ss,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 IN ('Starter Set Electric Orange', 'Starter Set Ocean Blue', 'Starter Set Hot Pink') THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_coloured_bottle,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Apple' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_apple,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Basil-Lemon' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_basil_lemon,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Blueberry' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_blueberry,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Cherry' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_cherry,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Coffee' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_coffee,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Cola' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_cola,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Cucumber' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_cucumber,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Elderflower' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_elderflower,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Lemon' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_lemon,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Lime' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_lime,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Lychee-Rose' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_lychee_rose,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Mango-Passionfruit' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_mango_passionfruit,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Orange-Passionfruit' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_orange_passionfruit,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Orange-Vanilla ' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_orange_vanilla ,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Peach' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_peach,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Pear' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_pear,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Pineapple' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_pineapple,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Pink Grapefruit' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_pink_grapefruit,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Raspberry-Lemon' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_raspberry_lemon,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Tangerine' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_tangerine,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Watermelon' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_watermelon,
        CASE 
            WHEN SUM(CASE WHEN shopify_product_categorisation.subcategory_3 = 'Wildberry' THEN 1 ELSE 0 END) >= 1 THEN 1
            ELSE 0
        END AS order_incl_wildberry,
        COUNT(order_line.title) AS basket_size,
        SUM(CASE WHEN shopify_product_categorisation.category = 'Hardware' THEN 1 ELSE 0 END) AS bottle_count
    FROM "airup_eu_dwh"."shopify_global"."fct_order_line" order_line
    LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_enriched_ngdpr" "order" ON order_line.order_id = "order".id
    LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" shopify_product_categorisation ON shopify_product_categorisation.sku = order_line.sku
    GROUP BY "order".email, "order".id, "order".created_at, "order".total_price
), klaviyo_dimensions_nps1 AS (
    SELECT 
        person.email,
        person.id AS customer_id,
        person.custom_nps_1,
        person.country,
        CURRENT_DATE - person.custom_first_purchase_date AS customer_duration_days,
        person.custom_first_purchase_date AS first_purchase_date
    FROM "airup_eu_dwh"."klaviyo_global"."dim_person" person 
    WHERE person.custom_nps_1 IS NOT NULL
), klaviyo_dimensions_nps1_events AS (
    SELECT 
        person.email,
        CASE WHEN event.type = 'Received Email' THEN MIN(datetime) END AS send_time,
        CASE WHEN event.type = 'Opened Email' THEN MIN(datetime) END AS open_time,
        CASE WHEN event.type = 'Clicked Email' THEN MIN(datetime) END AS response_time
    FROM "airup_eu_dwh"."klaviyo_global"."dim_event" event
    LEFT JOIN "airup_eu_dwh"."klaviyo_global"."dim_person" person ON person.id = event.person_id
    WHERE person.custom_nps_1 IS NOT NULL
    AND event.flow_id = 'WXbJw3'
    GROUP BY person.email, event."type", event.datetime 
), freshdesk_dimensions AS (
    SELECT DISTINCT
        person.email,
        CASE WHEN convo.from_email IS NOT NULL THEN 1 ELSE 0 END AS complaint
    FROM "airup_eu_dwh"."klaviyo_global"."dim_person" person
    LEFT JOIN "airup_eu_dwh"."freshdesk"."conversation" convo ON convo.from_email = person.email
)
SELECT
    klaviyo_dimensions_nps1.customer_id,
    klaviyo_dimensions_nps1.custom_nps_1,
    klaviyo_dimensions_nps1.country,
    klaviyo_dimensions_nps1.customer_duration_days,
    shopify_dimensions.total_price AS first_purchase_price,
    klaviyo_dimensions_nps1.first_purchase_date,
    shopify_dimensions.basket AS first_basket,
    shopify_dimensions.order_incl_bottle,
    shopify_dimensions.order_incl_ss,
    shopify_dimensions.order_incl_coloured_bottle,
    shopify_dimensions.order_incl_apple,
    shopify_dimensions.order_incl_basil_lemon,
    shopify_dimensions.order_incl_blueberry,
    shopify_dimensions.order_incl_cherry,
    shopify_dimensions.order_incl_coffee,
    shopify_dimensions.order_incl_cola,
    shopify_dimensions.order_incl_cucumber,
    shopify_dimensions.order_incl_elderflower,
    shopify_dimensions.order_incl_lemon,
    shopify_dimensions.order_incl_lime,
    shopify_dimensions.order_incl_lychee_rose,
    shopify_dimensions.order_incl_mango_passionfruit,
    shopify_dimensions.order_incl_orange_passionfruit,
    shopify_dimensions.order_incl_orange_vanilla ,
    shopify_dimensions.order_incl_peach,
    shopify_dimensions.order_incl_pear,
    shopify_dimensions.order_incl_pineapple,
    shopify_dimensions.order_incl_pink_grapefruit,
    shopify_dimensions.order_incl_raspberry_lemon,
    shopify_dimensions.order_incl_tangerine,
    shopify_dimensions.order_incl_watermelon,
    shopify_dimensions.order_incl_wildberry,
    shopify_dimensions.basket_size,
    shopify_dimensions.bottle_count,
    freshdesk_dimensions.complaint,
    klaviyo_dimensions_nps1_events.send_time,
    klaviyo_dimensions_nps1_events.open_time,
    klaviyo_dimensions_nps1_events.response_time
FROM shopify_dimensions
JOIN klaviyo_dimensions_nps1 ON shopify_dimensions.email = klaviyo_dimensions_nps1.email
LEFT JOIN freshdesk_dimensions ON shopify_dimensions.email = freshdesk_dimensions.email
LEFT JOIN klaviyo_dimensions_nps1_events ON klaviyo_dimensions_nps1_events.email = klaviyo_dimensions_nps1.email
AND shopify_dimensions.order_number = 1
AND klaviyo_dimensions_nps1.first_purchase_date IS NOT NULL