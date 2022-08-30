
    

   select
      * from 
   "airup_eu_dwh"."adwords_custom"."custom_report"

   
         where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."adwords_custom"."fct_adwords_custom_report")
         