WITH daily_keys AS
  (SELECT key_daily,
          ticker,
          date
     FROM {{ref('fct_daily')}}
    WHERE NOT interpolated_sep_row),

dim_fin_arq_keys AS
  (SELECT key_dim_fin,
          ticker,
          CAST(calendardate AS date) AS calendardate,
          CAST(datekey AS date) AS datekey
     FROM {{ref('dim_fin_arq')}}),

find_date AS
    (SELECT sep.key_daily,
            sep.ticker,
            sep.date,
            MAX(arq.datekey) AS dim_fin_arq_datekey
       FROM daily_keys AS sep
      INNER JOIN dim_fin_arq_keys AS arq
         ON sep.ticker = arq.ticker
        AND arq.datekey < sep.date
      GROUP BY 1,2,3
          )

SELECT key_daily,
       key_dim_fin,
       date - dim_fin_arq_datekey as days_since_last_arq
  FROM find_date
 INNER JOIN dim_fin_arq_keys
    ON find_date.ticker = dim_fin_arq_keys.ticker
   AND find_date.dim_fin_arq_datekey = dim_fin_arq_keys.datekey