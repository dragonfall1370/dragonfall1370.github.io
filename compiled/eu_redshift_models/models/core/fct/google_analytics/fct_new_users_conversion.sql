--- creator: Nham Dao
--- last modify: Nham Dao


WITH new_user_session AS
  (SELECT date_trunc('month', "date")::date AS "date",
          shop,
          sum(sessions) AS sessions_new_user
   FROM google_analytics.fct_funnel_performance_enriched
   WHERE user_type = 'New User'
     AND shopping_stage = 'ALL_VISITS'
   GROUP BY date_trunc('month', "date")::date,
            shop),
     session_with_transaction AS
  (SELECT date_trunc('month', "date")::date AS "date",
          shop,
          sum(sessions) AS sessions_with_transaction
   FROM google_analytics.fct_funnel_performance_enriched
   WHERE user_type = 'New User'
     AND shopping_stage = 'TRANSACTION'
   GROUP BY date_trunc('month', "date")::date,
            shop)
SELECT nus.*,
       swt.sessions_with_transaction
FROM new_user_session nus
LEFT JOIN session_with_transaction swt ON nus."date" = swt."date"
AND nus.shop = swt.shop