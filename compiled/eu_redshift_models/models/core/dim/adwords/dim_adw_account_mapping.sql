
 
 SELECT asp.customer_id AS customer_id,
        CASE
            WHEN asp.customer_id in  ('1513517584', '6942293362','9267315894','1257330976','4642198297') THEN 'at'::text
            WHEN asp.customer_id in  ('2669314894', '8446300411', '1057184937', '9555900311', '7431281972') THEN 'fr'::text
            WHEN asp.customer_id in  ('2774139193', '6070064242', '7922146894') THEN 'ch'::text
            WHEN asp.customer_id in  ('7536728545', '5083761080', '5088644752') THEN 'de'::text
            WHEN asp.customer_id in  ('5251971882', '4103725029', '7779431194') THEN 'nl'::text
            WHEN asp.customer_id in  ('8386509409', '2983000860', '6560483731') THEN 'uk'::text
            WHEN asp.customer_id in  ('8039303013', '9928631042', '1672912470') THEN 'it'::text
            WHEN asp.customer_id in  ('4722679303', '6124469409', '3323297903') THEN 'se'::text
            WHEN asp.customer_id in  ('9996140770', '4501455345') THEN 'be'::text
            WHEN asp.customer_id in  ('3551404595', '8806175260') THEN 'cz'::text 
            WHEN asp.customer_id in  ('5961710542', '4215407409') THEN 'us'::text            
            ELSE NULL::text
        END AS country,
        CASE
            WHEN asp.customer_id in  ('6942293362', '7922146894', '5083761080', '5088644752', '9555900311', 
                                      '7431281972', '7779431194', '8386509409', '9928631042','9267315894',
                                      '6124469409', '9996140770', '3551404595', '5961710542') THEN 'search'::text
            WHEN asp.customer_id in  ('1513517584', '2669314894', '8446300411', '1057184937', '2774139193', 
                                      '7536728545', '4103725029', '6560483731', '5880821145', '8039303013',
                                      '1257330976', '1672912470', '4642198297', '6070064242', '2983000860',
                                      '5251971882', '4501455345', '4722679303', '3323297903', '8806175260', '4215407409') THEN 'display_youtube'::text
            ELSE NULL::text
        END AS account_type
   FROM "airup_eu_dwh"."adwords_new_api"."account_stats_prebuilt" asp
  GROUP BY 1,2