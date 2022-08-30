

with 
final_table AS (
         SELECT thismonth.pod_flavour_first,
            thismonth.country_abbreviation,
            thismonth.product_type,
            thismonth.customers,
            thismonth.timeframe,
            thismonth.returning_customers_7_days,
            thismonth.returning_customers_30_days,
            thismonth.returning_customers_90_days,
            thismonth.returning_customer_overall
           FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_thismonth" thismonth
        UNION ALL
         SELECT thisquarter.pod_flavour_first,
            thisquarter.country_abbreviation,
            thisquarter.product_type,
            thisquarter.customers,
            thisquarter.timeframe,
            thisquarter.returning_customers_7_days,
            thisquarter.returning_customers_30_days,
            thisquarter.returning_customers_90_days,
            thisquarter.returning_customer_overall
           FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_thisquarter" thisquarter
        UNION ALL
         SELECT thisyear.pod_flavour_first,
            thisyear.country_abbreviation,
            thisyear.product_type,
            thisyear.customers,
            thisyear.timeframe,
            thisyear.returning_customers_7_days,
            thisyear.returning_customers_30_days,
            thisyear.returning_customers_90_days,
            thisyear.returning_customer_overall
           FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_thisyear" thisyear
        UNION ALL
         SELECT thisall.pod_flavour_first,
            thisall.country_abbreviation,
            thisall.product_type,
            thisall.customers,
            thisall.timeframe,
            thisall.returning_customers_7_days,
            thisall.returning_customers_30_days,
            thisall.returning_customers_90_days,
            thisall.returning_customer_overall
           FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_thisall" thisall
        UNION ALL
         SELECT this30days.pod_flavour_first,
            this30days.country_abbreviation,
            this30days.product_type,
            this30days.customers,
            this30days.timeframe,
            this30days.returning_customers_7_days,
            this30days.returning_customers_30_days,
            this30days.returning_customers_90_days,
            this30days.returning_customer_overall
           FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_this30" this30days
        UNION ALL
         SELECT this90days.pod_flavour_first,
            this90days.country_abbreviation,
            this90days.product_type,
            this90days.customers,
            this90days.timeframe,
            this90days.returning_customers_7_days,
            this90days.returning_customers_30_days,
            this90days.returning_customers_90_days,
            this90days.returning_customer_overall
           FROM "airup_eu_dwh"."flavour_analysis"."fct_returning_customers_by_first_flavour_this90" this90days
           )
 SELECT
 	final_table.pod_flavour_first,
    final_table.country_abbreviation,
    final_table.product_type,
    final_table.customers,
    final_table.timeframe,
    final_table.returning_customers_7_days,
    final_table.returning_customers_30_days,
    final_table.returning_customers_90_days,
    final_table.returning_customer_overall
   FROM final_table