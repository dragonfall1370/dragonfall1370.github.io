-- edited by YuShih Hsieh





WITH cml_ecommerce AS (
         SELECT ecommerce."event_date"::date AS event_date,
            ecommerce.sales_channel,
            ecommerce.channel,
            ecommerce.event,
            ecommerce.details,
            ecommerce.main_category,
            ecommerce.subcategory,
            ecommerce.logged_by,
            ecommerce.team,
            ecommerce.country
           FROM "airup_eu_dwh"."google_sheets"."ecommerce" ecommerce
        ), cml_performance_marketing AS (
         SELECT performance_marketing.event_date::date AS event_date,
            performance_marketing.sales_channel,
            performance_marketing.channel,
            performance_marketing.event,
            performance_marketing.details,
            performance_marketing.main_category,
            performance_marketing.subcategory,
            performance_marketing.logged_by,
            performance_marketing.team,
            performance_marketing.country
           FROM "airup_eu_dwh"."google_sheets"."performance_marketing" performance_marketing
        ), cml_influencer AS (
         SELECT influencer.event_date::date AS event_date,
            influencer.sales_channel,
            influencer.channel,
            influencer.event,
            influencer.details,
            influencer.main_category,
            influencer.subcategory,
            influencer.logged_by,
            influencer.team,
            influencer.country
           FROM "airup_eu_dwh"."google_sheets"."influencer" influencer
        ), cml_sales AS (
         SELECT sales.event_date,
            sales.sales_channel,
            sales.channel,
            sales.event,
            sales.details,
            sales.main_category,
            sales.subcategory,
            sales.logged_by,
            sales.team,
            sales.country
           FROM "airup_eu_dwh"."google_sheets"."sales" sales
        ), cml_crm AS (
         SELECT crm.event_date,
            crm.sales_channel,
            crm.channel,
            crm.event,
            crm.details,
            crm.main_category,
            crm.subcategory,
            crm.logged_by,
            crm.team,
            crm.country
           FROM "airup_eu_dwh"."google_sheets"."crm" crm
        ), cml_social_organic AS (
         SELECT social_organic.event_date,
            social_organic.sales_channel,
            social_organic.channel,
            social_organic.event,
            social_organic.details,
            social_organic.main_category,
            social_organic.subcategory,
            social_organic.logged_by,
            social_organic.team,
            social_organic.country
           FROM "airup_eu_dwh"."google_sheets"."social_organic" social_organic
        ), cml_pr AS (
         SELECT pr.event_date,
            pr.sales_channel,
            pr.channel,
            pr.event,
            pr.details,
            pr.main_category,
            pr.subcategory,
            pr.logged_by,
            pr.team,
            pr.country
           FROM "airup_eu_dwh"."google_sheets"."pr" pr
        ), marketing_log AS (
         SELECT cml_ecommerce.event_date,
            cml_ecommerce.sales_channel,
            cml_ecommerce.channel,
            cml_ecommerce.event,
            cml_ecommerce.details,
            cml_ecommerce.main_category,
            cml_ecommerce.subcategory,
            cml_ecommerce.logged_by,
            cml_ecommerce.team,
            cml_ecommerce.country
           FROM cml_ecommerce
        UNION ALL
         SELECT cml_performance_marketing.event_date,
            cml_performance_marketing.sales_channel,
            cml_performance_marketing.channel,
            cml_performance_marketing.event,
            cml_performance_marketing.details,
            cml_performance_marketing.main_category,
            cml_performance_marketing.subcategory,
            cml_performance_marketing.logged_by,
            cml_performance_marketing.team,
            cml_performance_marketing.country
           FROM cml_performance_marketing
        UNION ALL
         SELECT cml_influencer.event_date,
            cml_influencer.sales_channel,
            cml_influencer.channel,
            cml_influencer.event,
            cml_influencer.details,
            cml_influencer.main_category,
            cml_influencer.subcategory,
            cml_influencer.logged_by,
            cml_influencer.team,
            cml_influencer.country
           FROM cml_influencer
        UNION ALL
         SELECT cml_sales.event_date,
            cml_sales.sales_channel,
            cml_sales.channel,
            cml_sales.event,
            cml_sales.details,
            cml_sales.main_category,
            cml_sales.subcategory,
            cml_sales.logged_by,
            cml_sales.team,
            cml_sales.country
           FROM cml_sales
        UNION ALL
         SELECT cml_crm.event_date,
            cml_crm.sales_channel,
            cml_crm.channel,
            cml_crm.event,
            cml_crm.details,
            cml_crm.main_category,
            cml_crm.subcategory,
            cml_crm.logged_by,
            cml_crm.team,
            cml_crm.country
           FROM cml_crm
        UNION ALL
         SELECT cml_social_organic.event_date,
            cml_social_organic.sales_channel,
            cml_social_organic.channel,
            cml_social_organic.event,
            cml_social_organic.details,
            cml_social_organic.main_category,
            cml_social_organic.subcategory,
            cml_social_organic.logged_by,
            cml_social_organic.team,
            cml_social_organic.country
           FROM cml_social_organic
        UNION ALL
         SELECT cml_pr.event_date,
            cml_pr.sales_channel,
            cml_pr.channel,
            cml_pr.event,
            cml_pr.details,
            cml_pr.main_category,
            cml_pr.subcategory,
            cml_pr.logged_by,
            cml_pr.team,
            cml_pr.country
           FROM cml_pr
        )
 SELECT marketing_log.event_date,
    marketing_log.sales_channel,
    marketing_log.channel,
    marketing_log.event,
    marketing_log.details,
    marketing_log.main_category,
    marketing_log.subcategory,
    marketing_log.logged_by,
    marketing_log.team,
    marketing_log.country
   FROM marketing_log
  WHERE marketing_log.event::text <> 'NA'::text