---Authors: YuShih Hsieh
---Last Modified by: YuShih Hsieh

---###################################################################################################################
        -- this view contains the information on the customer_id, country and account_type
---###################################################################################################################

 
 

 SELECT asp.customer_id AS customer_id,
        CASE
            WHEN asp.customer_id in  ('1513517584', '6942293362','9267315894','1257330976','4642198297') THEN 'AT'::text
            WHEN asp.customer_id in  ('2669314894', '8446300411', '1057184937', '9555900311', '7431281972') THEN 'FR'::text
            WHEN asp.customer_id in  ('2774139193', '6070064242', '7922146894') THEN 'CH'::text
            WHEN asp.customer_id in  ('7536728545', '5083761080', '5088644752') THEN 'DE'::text
            WHEN asp.customer_id in  ('5251971882', '4103725029', '7779431194') THEN 'NL'::text
            WHEN asp.customer_id in  ('8386509409', '2983000860', '6560483731') THEN 'UK'::text
            WHEN asp.customer_id in  ('8039303013', '9928631042', '1672912470') THEN 'IT'::text
            WHEN asp.customer_id in  ('4722679303', '6124469409', '3323297903') THEN 'SE'::text
            WHEN asp.customer_id in  ('9996140770', '4501455345') THEN 'BE'::text
            WHEN asp.customer_id in  ('3551404595', '8806175260') THEN 'CZ'::text 
            WHEN asp.customer_id in  ('5961710542', '4215407409') THEN 'US'::text
            ELSE NULL::text
        END AS country,
        CASE
            WHEN asp.customer_id in  ('7922146894', '5088644752', '9555900311', '7431281972', '7779431194', '8386509409', '9928631042', '9267315894', '6942293362', '6124469409', '9996140770', '5961710542', '3551404595') THEN 'search'::text
            WHEN asp.customer_id in  ('4642198297', '8446300411', '1672912470', '6070064242', '2983000860', '5251971882', '5083761080', '4722679303') THEN 'display'::text
            WHEN asp.customer_id in  ('6560483731', '8039303013', '2669314894', '7536728545', '2774139193', '1257330976', '4103725029', '3323297903', '4501455345', '4215407409', '8806175260') THEN 'youtube'::text
            ELSE NULL::text
        END AS account_type
   FROM "airup_eu_dwh"."adwords_new_api"."account_stats_prebuilt" asp
  GROUP BY 1,2