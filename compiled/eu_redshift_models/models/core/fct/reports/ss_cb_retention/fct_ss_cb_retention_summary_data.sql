---Authors: Nham Dao 

---Last Modified by: Nham Dao 

 
 
 
 

--/*################################################################# 

--this view includes information regarding customers who bought starter set or color bottle in their first order  (only consider customers who  

--bought exactly 1 starter set/colored bottle in their first purchase), together with the detail order line of these customers 

--in the first and second purchase (if applicable) 

--#################################################################*/ 
 


WITH customer_order_id_first_purchase AS
  (SELECT customer_id,
          country_fullname,
          id,
          created_at
   FROM
     (SELECT customer_id,
             country_fullname,
             id,
             created_at,
             row_number() OVER (PARTITION BY customer_id,
                                             country_fullname
                                ORDER BY created_at) AS id_ranked
      FROM shopify_global.fct_order_enriched foe
      WHERE financial_status::text in ('paid',
                                       'partially_refunded')) t1
   WHERE id_ranked = 1),
first_customer_order_ss_cb AS
  (SELECT customer_id,
          id,
          country_fullname AS country,
          created_at
   FROM
     (SELECT order_enriched.customer_id,
             order_enriched.country_fullname,
             order_enriched.id,
             order_enriched.created_at
      FROM customer_order_id_first_purchase order_enriched
      JOIN shopify_global.fct_order_line order_line ON order_enriched.id = order_line.order_id
      WHERE order_line.sku::text in ('140000015',
                                     '100000040',
                                     '140000016',
                                     '140000017',
                                     '100000033',
                                     'a-1000096',
                                     '100000002',
                                     '100000005',
                                     '100000055',
                                     '100000003',
                                     '100000004',
                                     '140000014',
                                     '140000023',
                                     '140000024',
                                     '100000007',
                                     '100000041',
                                     '140000021',
                                     '140000020',
                                     '100000056',
                                     '100000008',
                                     '140000022',
                                     '100000006',
                                     '140000010',
                                     '140000011' ,
                                     '140000009')
      GROUP BY order_enriched.customer_id,
               order_enriched.country_fullname,
               order_enriched.id,
               order_enriched.created_at) AS t1) ,
first_order_only_one_ss_cb AS
  (SELECT fcosc.customer_id,
          fcosc.id,
          fcosc.country,
          fcosc.created_at,
          sum(ol.quantity) AS SUM
   FROM first_customer_order_ss_cb fcosc
   LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" ol ON fcosc.id = ol.order_id
   WHERE (ol.sku::text IN
            (SELECT spc_1.sku
             FROM "airup_eu_dwh"."shopify_global"."shopify_product_categorisation" spc_1
             WHERE spc_1.subcategory_1::text = 'Starter Set'::text))--AND spc_1.product_status = 'active'::text

   GROUP BY fcosc.customer_id,
            fcosc.id,
            fcosc.country,
            fcosc.created_at
   HAVING sum(ol.quantity) = 1::double precision),
all_order_from_first_customer_order_only_one_ss_bb AS
  (SELECT oe.customer_id,
          oe.country_fullname,
          oe.id,
          oe.created_at,
          ol2.name,
          ol2.sku
   FROM "airup_eu_dwh"."shopify_global"."fct_order_enriched" oe
   LEFT JOIN "airup_eu_dwh"."shopify_global"."fct_order_line" ol2 ON oe.id = ol2.order_id
   WHERE (oe.financial_status::text in ('paid',
                                        'partially_refunded'))
     AND (oe.customer_id IN
            (SELECT first_order_only_one_ss_cb.customer_id
             FROM first_order_only_one_ss_cb)) ),
up_to_second_order_from_first_order AS
  (SELECT t1.customer_id,
          t1.country_fullname,
          t1.id,
          t1.created_at,
          t1.name,
          t1.sku,
          t1.rank_order
   FROM
     (SELECT all_order_from_first_customer_order_only_one_ss_bb.customer_id,
             all_order_from_first_customer_order_only_one_ss_bb.country_fullname,
             all_order_from_first_customer_order_only_one_ss_bb.id,
             all_order_from_first_customer_order_only_one_ss_bb.created_at,
             all_order_from_first_customer_order_only_one_ss_bb.name,
             all_order_from_first_customer_order_only_one_ss_bb.sku,
             dense_rank() OVER (PARTITION BY all_order_from_first_customer_order_only_one_ss_bb.customer_id
                                ORDER BY all_order_from_first_customer_order_only_one_ss_bb.created_at) AS rank_order
      FROM all_order_from_first_customer_order_only_one_ss_bb) t1
   WHERE t1.rank_order <= 2 ),
time_diff AS
  (SELECT utso.customer_id,
          utso.country_fullname,
          utso.id,
          utso.created_at,
          utso.name,
          utso.sku,
          utso.rank_order,
          datediff(DAY, fo.created_at::date, utso.created_at::date) AS timediff
   FROM up_to_second_order_from_first_order utso
   LEFT JOIN first_order_only_one_ss_cb fo ON utso.customer_id = fo.customer_id
   AND utso.country_fullname::text = fo.country::text
   WHERE datediff(DAY, fo.created_at::date, utso.created_at::date) >= 0::double precision )
SELECT td.customer_id,
       td.country_fullname,
       td.id,
       td.created_at,
       td.name,
       td.sku,
       td.rank_order,
       td.timediff,
       spc.category,
       spc.subcategory_1,
       spc.subcategory_2,
       spc.subcategory_3
FROM time_diff td
LEFT JOIN "airup_eu_dwh"."shopify_global"."shopify_product_categorisation"spc ON td.sku::text = spc.sku::text
WHERE spc.subcategory_1 IS NOT NULL