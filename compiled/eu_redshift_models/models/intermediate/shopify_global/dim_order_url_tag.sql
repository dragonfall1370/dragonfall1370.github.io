

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_de"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_ch"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_fr"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_it"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_nl"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_uk"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_se"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_at"."order_url_tag"

UNION all

select
    order_id,
    key,
    value
from
    "airup_eu_dwh"."shopify_us"."order_url_tag"