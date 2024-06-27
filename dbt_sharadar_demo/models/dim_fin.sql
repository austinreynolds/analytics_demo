SELECT ticker || '_' || dimension || '_' || cast(calendardate as date) || '_' || cast(datekey as date) AS key_dim_fin,
       ticker,
       dimension,
       cast(calendardate as date) AS calendardate,
       cast(datekey as date) AS datekey,
       {{get_sf1_currency_cols(False)}},
       {{get_sf1_ratio_cols()}}
  FROM {{ source('raw', 'sf1') }}