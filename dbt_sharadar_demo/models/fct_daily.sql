WITH sep_dates_all AS
    (SELECT DISTINCT CAST(date AS DATE) AS date FROM {{ ref('fct_prices_daily') }}),

sep_dates_keep AS
    (SELECT date
       FROM sep_dates_all
    QUALIFY RANK() OVER (PARTITION BY YEAR(date), MONTH(date) ORDER BY date) IN {{var('prices_days_of_month_to_keep')}}
         OR date = MAX(date) OVER (PARTITION BY TRUE)
          )

SELECT s.key_daily,
       s.ticker,
       s.date,
       s.closeadj,
       s.volume,
       s.interpolated_sep_row,
       d.mcap_daily,
       d.ev_daily,
       d.ev_over_mcap_daily,
       d.evebit_daily,
       d.evebitda_daily,
       d.pb_daily,
       d.pe_daily,
       d.ps_daily,
       d.mcap_rank,
       d.mcap_perc_rank,
       d.mcap_rank_rev,
       d.mcap_perc_rank_rev
  FROM {{ref('fct_prices_daily')}} AS s
  LEFT JOIN {{ref('fct_fin_daily')}} AS d
    ON s.key_daily = d.key_daily
 INNER JOIN sep_dates_keep AS dk
    ON s.date = dk.date
