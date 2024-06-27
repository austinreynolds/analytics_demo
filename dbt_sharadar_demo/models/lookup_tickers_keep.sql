SELECT DISTINCT
       ticker,
       name,
       sicsector,
       sicindustry,
       famaindustry,
       sector,
       industry
FROM {{ source('raw', 'tickers') }}
WHERE ticker IS NOT NULL
  AND "table" = 'SF1'
  AND category ilike '%domestic%'
  AND category ilike '%common stock%'
  AND category NOT ilike '%warrant%'