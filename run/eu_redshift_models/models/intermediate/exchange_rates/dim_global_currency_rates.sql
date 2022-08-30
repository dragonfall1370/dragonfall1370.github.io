
      
    delete from "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates"
    where (hash_id) in (
        select (hash_id)
        from "dim_global_currency_rates__dbt_tmp152416168125"
    );
    

    insert into "airup_eu_dwh"."odoo_currency"."dim_global_currency_rates" ("hash_id", "currency_id", "creation_datetime", "creation_date", "symbol", "currency_abbreviation", "currency_name", "conversion_rate_eur", "_fivetran_synced")
    (
        select "hash_id", "currency_id", "creation_datetime", "creation_date", "symbol", "currency_abbreviation", "currency_name", "conversion_rate_eur", "_fivetran_synced"
        from "dim_global_currency_rates__dbt_tmp152416168125"
    )
  