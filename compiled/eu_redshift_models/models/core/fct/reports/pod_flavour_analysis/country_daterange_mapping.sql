

SELECT DISTINCT 
    flavours_pod_freq.country_abbreviation,
    flavours_pod_freq.subset,
    COALESCE(country_system_account_mapping.country_fullname, 'ALL'::character varying) AS "coalesce"
FROM flavour_analysis.flavours_pod_freq
    LEFT JOIN public.country_system_account_mapping 
        ON flavours_pod_freq.country_abbreviation::text =
            country_system_account_mapping.country_abbreviation::text