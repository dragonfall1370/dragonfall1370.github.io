


SELECT bam.source || ' / ' || bam.medium AS "Source / Medium",
    bam.campaign AS "Campaign",
    sum(bam.users) AS "Users",
    sum(bam.sessions) AS "Sessions",
    sum(bam.transaction_revenue) AS "Revenue",
    sum(bam.transactions) AS "Transactions",
    sum(bam.transaction_revenue) / sum(NULLIF(bam.transactions, 0))::double precision AS "Avg. Order Value",
    sum(bam.transactions) / sum(NULLIF(bam.sessions, 0))::double precision AS "Ecommerce Conversion Rate",
    sum(bam.transaction_revenue) / sum(NULLIF(bam.transactions, 0))::double precision * (sum(bam.transactions)::double precision / sum(NULLIF(bam.sessions, 0)))::double precision AS "Per Session Value"
   FROM "airup_eu_dwh"."google_analytics"."fct_basic_acquisition_metrics" bam
  GROUP BY 1,2