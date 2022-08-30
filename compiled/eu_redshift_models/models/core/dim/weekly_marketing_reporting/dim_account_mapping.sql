

SELECT asp.customer_id,
        CASE
            WHEN asp.customer_id IN ('1513517584', '6942293362') THEN 'at'
            WHEN asp.customer_id IN ('2669314894', '8446300411', '1057184937', '9555900311', '7431281972') THEN 'fr'
            WHEN asp.customer_id IN ('2774139193', '6070064242', '7922146894') THEN 'ch'
            WHEN asp.customer_id IN ('7536728545', '5083761080', '5088644752') THEN 'de'
            WHEN asp.customer_id IN ('5251971882', '4103725029', '7779431194') THEN 'nl'
            WHEN asp.customer_id IN ('8386509409', '2983000860', '6560483731') THEN 'uk'
            WHEN asp.customer_id IN ('8039303013', '9928631042', '1672912470') THEN 'it'
            ELSE NULL
        END AS country,
        CASE
            WHEN asp.customer_id IN ('6942293362', '7922146894', '5083761080', '5088644752', '9555900311', '7431281972', '7779431194', '8386509409', '9928631042') THEN 'search'
            WHEN asp.customer_id IN ('1513517584', '2669314894', '8446300411', '1057184937', '2774139193', '7536728545', '4103725029', '6560483731', '5880821145', '8039303013') THEN 'display_youtube'
            ELSE NULL
        END AS account_type
   FROM "airup_eu_dwh"."adwords"."account_stats_prebuilt" asp
  GROUP BY asp.customer_id, (
        CASE
            WHEN asp.customer_id IN ('1513517584', '6942293362') THEN 'at'
            WHEN asp.customer_id IN ('2669314894', '1057184937', '9555900311', '7431281972') THEN 'fr'
            WHEN asp.customer_id IN ('2774139193', '7922146894') THEN 'ch'
            WHEN asp.customer_id IN ('7536728545', '5088644752') THEN 'de'
            WHEN asp.customer_id IN ('5251971882', '4103725029', '7779431194') THEN 'nl'
            WHEN asp.customer_id IN ('8386509409', '2983000860', '6560483731') THEN 'uk'
            WHEN asp.customer_id IN ('8039303013', '9928631042', '1672912470') THEN 'it'
            ELSE NULL
        END)