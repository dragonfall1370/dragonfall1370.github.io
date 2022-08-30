 

SELECT  
       _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_de"
   where 1=1  
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_de" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
      
UNION all

SELECT  
       _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_at"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_at" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
      
UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_fr"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_fr" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   
UNION all

 SELECT 
     _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_nl"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_nl" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))

UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_ch"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_ch" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   
UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_uk"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_uk" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   
UNION all

 SELECT 

      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_it"
   where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_it" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))
   


UNION all

 SELECT 
      _fivetran_synced
       ,creation_date
       ,amount
       ,amount_chf
       ,amount_gbp
       ,amount_sek
       ,amount_set
       ,id
       ,kind
       ,order_id 
       ,reason
       ,refund_id
       ,tax_amount
       ,tax_amount_chf
       ,tax_amount_gbp
       ,tax_amount_sek
       ,tax_amount_set
       ,shopify_shop
       ,currency_abbreviation
       ,conversion_rate_eur
       
   FROM "airup_eu_dwh"."shopify_global"."order_adjustment_se"
    where 1=1
   and order_id in (select distinct id from "airup_eu_dwh"."shopify_global"."order_se" where date(created_at) >= (select max(three_month_ago) from "airup_eu_dwh"."shopify_global_live_con"."fct_months_back_condition"))