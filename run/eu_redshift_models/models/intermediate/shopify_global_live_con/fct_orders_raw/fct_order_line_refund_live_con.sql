

  create  table
    "airup_eu_dwh"."shopify_global_live_con"."fct_order_line_refund_live_con__dbt_tmp"
    
    
    
  as (
     


SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_de"
 where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_de"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_de" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )
   
   
UNION all

SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_at"
 where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_at"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_at" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )
   
UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_fr"
   where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_fr"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_fr" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )
   
UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_nl"
    where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_nl"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_nl" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )

UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_ch"
    where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_ch"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_ch" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )
   
UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_uk"
    where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_uk"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_uk" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )

   UNION all

 SELECT 
     _fivetran_synced,
     creation_date,
     id,
     location_id,
     order_line_id,
     quantity,
     refund_id,
     restock_type,
     subtotal,
     subtotal_chf,
     subtotal_gbp,
      subtotal_sek,
     subtotal_set,
     total_tax,
     total_tax_chf,
     total_tax_gbp,
      total_tax_sek,
     total_tax_set,
     shopify_shop,
    currency_abbreviation,
     conversion_rate_eur
   FROM "airup_eu_dwh"."shopify_global"."order_line_refund_se"
    where 1=1
   and order_line_id in (select distinct id 
                        from "airup_eu_dwh"."shopify_global"."order_line_se"
                            where order_id in (
                                select distinct id 
                                from "airup_eu_dwh"."shopify_global"."order_se" 
                                where date(created_at) >= (select max(three_month_ago) 
                                                            from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"
                                                        )
                            )
                        )
  );