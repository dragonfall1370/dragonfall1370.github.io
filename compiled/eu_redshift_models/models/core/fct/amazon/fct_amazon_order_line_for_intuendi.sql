---Authors: Nham Dao
---Last revised: Nham Dao


---###################################################################################################################

        ---View that contains amazon order line in intuendi format---
 

---###################################################################################################################

 


select amazon_order_id as "Order ID"
,sku as "SKU", purchase_date::date as "Date"
, quantity_shipped as "Quantity"
,item_price  as "Amount"
, country_abbreviation as "Region"
, concat('AMZ ',upper(split_part(split_part(buyer_email, '@', 2),'.',3))) as "Location"
from "airup_eu_dwh"."amazon"."fct_orders_fulfilled_shipments_enriched"