

  create view "airup_eu_dwh"."shopify_global"."fct_order_line_refund_w_created_at__dbt_tmp" as (
    ---Authors: YuShih Hsieh
---Last Modified by: Long Vu

---###################################################################################################################
        -- add created_at column to fct_order_line_refund, report will be shown more correctly
---###################################################################################################################

 

select folr.*
        ,fol.groupid	
        ,fol.groupprice
        ,fol.grouptype
        ,fol.groupitemtype
		,fo.created_at as report_date_w_tz
		,fo.created_at_premain as report_date
from "airup_eu_dwh"."shopify_global"."fct_order_line_refund"  folr 
join "airup_eu_dwh"."shopify_global"."fct_order_line" fol on folr.order_line_id = fol.id 
join (select "order_pre".id, "order_pre".created_at
        ,case when tz.timezone_citiname is not null then convert_timezone(tz.timezone_citiname, "order_pre".created_at::timestamp)
              when tz.timezone_shortname is not null then convert_timezone(tz.timezone_shortname, "order_pre".created_at::timestamp)
            else convert_timezone('CET', "order_pre".created_at::timestamp)
        end as created_at_premain
        ,case when tz.timezone_citiname is not null then convert_timezone(tz.timezone_citiname, "order_pre".updated_at::timestamp)
              when tz.timezone_shortname is not null then convert_timezone(tz.timezone_shortname, "order_pre".updated_at::timestamp)
            else convert_timezone('CET', "order_pre".updated_at::timestamp)
        end as updated_at_premain
from "airup_eu_dwh"."shopify_global"."fct_order" "order_pre"
left join "airup_eu_dwh"."public"."timezone_configuration" tz on "order_pre".shopify_shop = tz.webshop_name) fo on fol.order_id = fo.id
  ) with no schema binding;
