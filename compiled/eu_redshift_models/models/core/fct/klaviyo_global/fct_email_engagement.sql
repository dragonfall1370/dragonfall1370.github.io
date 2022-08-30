

SELECT 
	CASE 
			WHEN custom_type IS NULL THEN NULL 
			WHEN custom_type LIKE '%person|lookup:%' THEN NULL
			ELSE split_part(custom_type, ' ',2) 
	END AS campaign_id	
	, mail_client 
	, platform 
	, ROUND(AVG(CAST(read_seconds AS FLOAT)), 2) AS avg_reading_time
	, SUM(CASE WHEN read_category = 'read' THEN 1 ELSE 0 END)::double precision / COUNT(*) AS percent_read
	, SUM(CASE WHEN read_category = 'skimmed' THEN 1 ELSE 0 END)::double precision / COUNT(*) AS percent_skimmed
	, SUM(CASE WHEN read_category = 'glanced' THEN 1 ELSE 0 END)::double precision / COUNT(*) AS percent_glanced
	, SUM(CASE WHEN read_category = 'unknown' THEN 1 ELSE 0 END)::double precision / COUNT(*) AS percent_unknown
FROM "airup_eu_dwh"."litmus"."litmus_sftp_data"
WHERE 1=1 AND custom_type IS NOT NULL AND custom_type NOT LIKE '%person|lookup:%' AND read_category <> 'unknown'
GROUP BY CASE WHEN custom_type IS NULL THEN NULL WHEN custom_type LIKE '%person|lookup:%' THEN NULL ELSE split_part(custom_type, ' ',2) END, mail_client, platform