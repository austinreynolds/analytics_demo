-- move currency cols to dollars

SELECT ticker || '_' || CAST(date AS DATE) AS key_daily,
       ticker,
       CAST(date AS DATE) AS date,
       marketcap*1000*1000 AS mcap_daily,
       ev*1000*1000 AS ev_daily,
       ev/marketcap AS ev_over_mcap_daily,
       evebit AS evebit_daily,
       evebitda AS evebitda_daily,
       pb AS pb_daily,
       pe AS pe_daily,
       ps AS ps_daily,
       RANK() OVER (PARTITION BY date ORDER BY marketcap desc) AS mcap_rank,
       PERCENT_RANK() OVER (PARTITION BY date ORDER BY marketcap desc) AS mcap_perc_rank,
       RANK() OVER (PARTITION BY date ORDER BY marketcap) AS mcap_rank_rev,
       PERCENT_RANK() OVER (PARTITION BY date ORDER BY marketcap) AS mcap_perc_rank_rev
  FROM {{ source('raw', 'daily') }}
