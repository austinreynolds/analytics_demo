-- Logic:
-- ary_lookup: Find latest ARY key per ART key. Not just the latest year, but the latest copy,
--             by checking the date it was reported (datekey)
-- find_ary_keyset: Use ARY latest key to "scan backwards" & find up to five matching years.
--                  Use order of ops (JOIN happens before QUALIFY) to further subset to latest version of ARY rows.
-- final select stmt: Throw out keysets with fewer than five ARY members.

WITH ary_lookup AS
   (SELECT t1.key_dim_fin AS key_dim_fin_art,
           t2.key_dim_fin AS key_dim_fin_ary,
           t2.ticker,
           t2.calendardate AS calendardate_latest_ary
      FROM {{ref('dim_fin_art')}} AS t1
     INNER JOIN {{ref('dim_fin_ary')}} AS t2
        ON t1.ticker = t2.ticker
       AND t2.datekey <= t1.datekey
   QUALIFY t2.calendardate = MAX(t2.calendardate) OVER (PARTITION BY t1.key_dim_fin)
       AND t2.datekey = MAX(t2.datekey) OVER (PARTITION BY t1.key_dim_fin, t2.calendardate)
         ),

find_ary_keyset AS
   (SELECT t1.key_dim_fin_art,
           t2.key_dim_fin AS key_dim_fin_ary,
           DENSE_RANK() OVER (PARTITION BY t1.key_dim_fin_art ORDER BY t2.calendardate) AS ord_calendardate_ary
      FROM ary_lookup AS t1
     INNER JOIN {{ref('dim_fin_ary')}} AS t2
        ON t1.ticker = t2.ticker
       AND (t2.calendardate in (t1.calendardate_latest_ary - interval '1 year', t1.calendardate_latest_ary - interval '2 years', t1.calendardate_latest_ary - interval '3 years', t1.calendardate_latest_ary - interval '4 years')
        OR t1.key_dim_fin_ary = t2.key_dim_fin)
   QUALIFY t2.datekey = MAX(t2.datekey) OVER (PARTITION BY t1.key_dim_fin_art, t2.calendardate)
         )

 SELECT *
   FROM find_ary_keyset
QUALIFY 5 = COUNT(1) OVER (PARTITION BY key_dim_fin_art)
