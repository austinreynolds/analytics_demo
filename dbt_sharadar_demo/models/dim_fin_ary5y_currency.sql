SELECT b.key_dim_fin_art AS key_dim_fin,
       {{get_sf1_currency_cols(False)}},
  FROM {{ref('brg_dim_fin_art_dim_fin_ary5y')}} AS b
 INNER JOIN {{ref('dim_fin_ary')}} AS a
    ON b.key_dim_fin_ary = a.key_dim_fin
