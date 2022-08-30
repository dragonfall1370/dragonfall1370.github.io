--legacy: migrated by tomas k.



with
  ALL_KOLA_ORDERS_PER_CUSTOMER as (
    select
      ORDER_ENRICHED.CUSTOMER_ID,
      ORDER_LINE.ORDER_ID,
      ORDER_ENRICHED.CREATED_AT,
      ORDER_ENRICHED.SHOPIFY_SHOP,
      min(ORDER_ENRICHED.CREATED_AT) over (
        partition by ORDER_ENRICHED.CUSTOMER_ID, ORDER_ENRICHED.SHOPIFY_SHOP
        order by ORDER_ENRICHED.CREATED_AT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) MIN_ORDER_DATE_PER_CUSTOMER,
      MAPPING.SUBCATEGORY_1 STARTER_SET_FLAG
    from "airup_eu_dwh"."shopify_global"."fct_order_enriched" ORDER_ENRICHED
      left outer join "airup_eu_dwh"."shopify_global"."fct_order_line" ORDER_LINE
        on ORDER_ENRICHED.ID = ORDER_LINE.ORDER_ID
      left outer join shopify_global.seed_shopify_product_categorisation MAPPING
        on MAPPING.SKU = ORDER_LINE.SKU
      left outer join shopify_global.seed_shopify_product_categorisation_ss_and_bundle_content MAPPING_SS
        on MAPPING.SKU = MAPPING_SS.SKU
    where (
      ORDER_ENRICHED.FINANCIAL_STATUS in (
        'paid', 'partially_refunded'
      )
      and ORDER_ENRICHED.CUSTOMER_ID is not NULL
      and MAPPING_SS.SUBCATEGORY_4 = 'Kola' or MAPPING.subcategory_2 like '%Kola%'
    )
  )
select
  CUSTOMER_ID,
  ORDER_ID,
  CREATED_AT,
  MIN_ORDER_DATE_PER_CUSTOMER,
  SHOPIFY_SHOP,
  max(case
    when (
      MIN_ORDER_DATE_PER_CUSTOMER = CREATED_AT
      and STARTER_SET_FLAG = 'Starter Set'
    ) then 1
    else 0
  end) over (partition by CUSTOMER_ID, SHOPIFY_SHOP) STARTER_SETS_IN_FIRST_ORDER
from ALL_KOLA_ORDERS_PER_CUSTOMER