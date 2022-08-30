
      

  create  table
    "airup_eu_dwh"."shopify_global"."dim_order_discount_code__dbt_tmp"
    
    
    
  as (
    

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_de"."order_discount_code"

union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_at"."order_discount_code"

union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_ch"."order_discount_code"

union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_fr"."order_discount_code"

union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_it"."order_discount_code"

union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_nl"."order_discount_code"

union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_uk"."order_discount_code"


union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_se"."order_discount_code"


union all

select
    order_id,
    index,
    code,
    type,
    amount
from
    "airup_eu_dwh"."shopify_us"."order_discount_code"
  );
  