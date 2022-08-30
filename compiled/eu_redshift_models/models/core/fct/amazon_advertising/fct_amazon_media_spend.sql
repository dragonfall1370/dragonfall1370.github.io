

 WITH unionised_view AS (
         SELECT 
            "date",
            profile_country_code,
            sum(cost) AS media_spend
           FROM "airup_eu_dwh"."amazon_advertising"."sponsored_brands_by_campaign"
          GROUP BY "date", profile_country_code
        UNION ALL
         SELECT 
            "date",
            profile_country_code,
            sum(cost) AS media_spend
           FROM "airup_eu_dwh"."amazon_advertising"."sponsored_brands_video_by_campaign"
          GROUP BY "date", profile_country_code
        UNION ALL
         SELECT "date",
            profile_country_code,
            sum(cost) AS media_spend
           FROM "airup_eu_dwh"."amazon_advertising"."sponsored_display_t_00030_by_campaign"
          GROUP BY "date", profile_country_code
        UNION ALL
         SELECT 
            "date",
            profile_country_code,
            sum(cost) AS media_spend
           FROM "airup_eu_dwh"."amazon_advertising"."sponsored_product_by_campaign"
          GROUP BY "date", profile_country_code
        )
 SELECT unionised_view."date",
    country_system_account_mapping.country_fullname AS country,
    sum(unionised_view.media_spend) AS amazon_media_spend,
    country_system_account_mapping.country_grouping
   FROM unionised_view
     LEFT JOIN "airup_eu_dwh"."public"."country_system_account_mapping" ON unionised_view.profile_country_code = country_system_account_mapping.country_abbreviation
  GROUP BY unionised_view."date", country_system_account_mapping.country_fullname, country_system_account_mapping.country_grouping