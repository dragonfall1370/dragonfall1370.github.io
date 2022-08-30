

         SELECT abe.date,
            abe.region,
            abe.channel,
            abe.channel_subcategory,
            abe.total_spend AS plan,
            dgacbc.total_spend AS actual
           FROM "airup_eu_dwh"."ads_budget_tracker"."fct_ad_budget_enriched" abe
             LEFT JOIN "airup_eu_dwh"."ads_budget_tracker"."fct_daily_ads_cost_by_country" dgacbc ON dgacbc.date = abe.date 
             AND dgacbc.region = abe.region::text AND dgacbc.channel = abe.channel AND dgacbc.channel_subcategory = abe.channel_subcategory
          ORDER BY abe.date desc, abe.region, abe.channel