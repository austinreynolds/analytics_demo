-- Daily stock prices. Interpolate rows beyond stocks' delistings using final price, for target calc purposes.

WITH sep_dates AS
    (SELECT DISTINCT date FROM {{source('raw', 'sep')}} ORDER BY date),

max_sep_dt AS
    (SELECT MAX(date) AS max_date FROM sep_dates),

max_sep_dt_per_ticker AS
    (SELECT ticker,
            MAX(date) AS max_date
       FROM {{ source('raw', 'sep') }}
      GROUP BY ticker
    ),

sep_delisted_tickers AS
    (SELECT msd.ticker, msd.max_date
       FROM max_sep_dt_per_ticker as msd
      INNER JOIN max_sep_dt
         ON msd.max_date < max_sep_dt.max_date
    ),

actions_delistings AS
    (SELECT ticker,
            MIN(date) AS first_delist_date
       FROM {{ source('raw', 'actions') }}
      WHERE action ILIKE '%delist%'
      GROUP BY 1
    ),

sep_ticker_delistings AS
    (SELECT sep_raw.*, sdt.max_date
       FROM sep_delisted_tickers AS sdt
      INNER JOIN actions_delistings AS ad
         ON sdt.ticker = ad.ticker
      INNER JOIN {{ source('raw', 'sep') }} sep_raw
         ON sdt.ticker = sep_raw.ticker
        AND sdt.max_date = sep_raw.date
    ),

sep_interp AS
    (SELECT ticker || '_' || CAST(sep_dates.date AS DATE) AS key_daily,
            d.ticker,
            CAST(sep_dates.date AS DATE) AS date,
            d.open,
            d.high,
            d.low,
            d.close,
            d.volume,
            d.closeadj,
            d.lastupdated,
            TRUE AS interpolated_sep_row
       FROM sep_ticker_delistings AS d
      INNER JOIN sep_dates
         ON d.max_date < sep_dates.date
        AND sep_dates.date - d.max_date < INTERVAL {{var('price_interpolation_interval')}}
    ),

sep_orig AS
    (SELECT ticker || '_' || CAST("date" AS DATE) AS key_daily,
            ticker,
            CAST("date" AS DATE) AS date,
            open,
            high,
            low,
            close,
            volume,
            closeadj,
            lastupdated,
            FALSE AS interpolated_sep_row
       FROM {{ source('raw', 'sep') }}
    )

SELECT * FROM sep_orig
UNION
SELECT * FROM sep_interp
