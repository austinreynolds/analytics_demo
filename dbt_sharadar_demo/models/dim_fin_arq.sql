-- Financials table TTM reports
-- ART means "as reported TTM" where TTM is "trailing twelve months"
-- These have quarterly frequency

SELECT *
  FROM {{ ref('dim_fin') }}
 WHERE dimension = 'ARQ'
 