WITH max_sep_dt AS
    (SELECT MAX(date) AS max_dt FROM {{ref('fct_daily')}}),

agg AS
  (SELECT s1.key_daily,
          s1.ticker,
          s1.date,
          s1.closeadj,
          AVG(s2.closeadj) AS future_closeadj,
          count(1) cnt
     FROM {{ref('fct_daily')}} s1
    INNER JOIN {{ref('fct_daily')}} s2
       ON s1.ticker = s2.ticker
      AND s2.date > s1.date
      AND s2.date <= s1.date + INTERVAL '1 year'
    INNER JOIN max_sep_dt
       ON TRUE
    WHERE s1.date < max_sep_dt.max_dt - INTERVAL '1 year'
      AND NOT s1.interpolated_sep_row
    GROUP BY 1,2,3,4
        ),

med AS
  (SELECT MEDIAN(cnt) AS median_cnt FROM agg),

spy_join AS
  (SELECT s1.date, 
          s1.closeadj,
          AVG(s2.closeadj) AS future_closeadj
     FROM {{source('raw', 'sfp')}} AS s1
    INNER JOIN {{source('raw', 'sfp')}} AS s2
       ON s1.ticker = s2.ticker
      AND s2.date > s1.date
      AND s2.date <= s1.date + INTERVAL '1 year'
    INNER JOIN max_sep_dt
       ON TRUE
    WHERE s1.date < max_sep_dt.max_dt - INTERVAL '1 year'
      AND s1.ticker = 'SPY'
    GROUP BY 1,2
        ),

spy_chg AS
  (SELECT date,
          (future_closeadj - closeadj)/closeadj AS spy_price_chg
     FROM spy_join
        ),

big_join AS
  (SELECT agg.key_daily,
          agg.ticker,
          agg.date,
          closeadj,
          future_closeadj,
          -- for diagnostics, I'm leaving the cnt, median_cnt and CASE statement
          cnt, median_cnt,
          CASE WHEN (cnt >= median_cnt - 7) AND (closeadj > 0)
               THEN (future_closeadj - closeadj) / closeadj
               ELSE NULL END AS price_chg,
          spy_chg.spy_price_chg       
     FROM agg
    INNER JOIN med
       ON TRUE
    INNER JOIN spy_chg
       ON agg.date = spy_chg.date
        )

SELECT *,
       price_chg - spy_price_chg AS excess_price_chg
  FROM big_join
