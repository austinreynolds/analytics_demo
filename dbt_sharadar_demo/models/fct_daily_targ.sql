SELECT s.key_daily,
       s.ticker,
       s.date,
       s.volume,
       s.closeadj,
       s.mcap_daily,
       s.ev_daily,
       t_1y.price_chg AS price_chg_1y,
       t_1y.excess_price_chg AS excess_price_chg_1y,
       sab.key_dim_fin,
       s.mcap_rank,
       s.mcap_perc_rank,
       s.mcap_rank_rev,
       s.mcap_perc_rank_rev
  FROM {{ref('fct_daily')}} AS s
  LEFT JOIN {{ref('fct_daily_target_1y')}} AS t_1y
    ON s.key_daily = t_1y.key_daily
 INNER JOIN {{ref('brg_fct_daily_dim_fin_art')}} AS sab
    ON s.key_daily = sab.key_daily
 WHERE NOT s.interpolated_sep_row
