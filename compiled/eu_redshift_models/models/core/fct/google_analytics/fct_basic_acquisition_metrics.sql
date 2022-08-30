
    
  
  select *
        FROM "airup_eu_dwh"."google_analytics"."basic_acquisition_metrics"

   
        where _fivetran_synced >= (select max(_fivetran_synced) from "airup_eu_dwh"."google_analytics"."fct_basic_acquisition_metrics")
        