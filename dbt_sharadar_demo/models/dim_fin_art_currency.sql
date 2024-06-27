SELECT key_dim_fin,
       {{get_sf1_currency_cols(False)}},
FROM {{ref('dim_fin_art')}}