-- Financials table annual reports
-- ARY means "as reported yearly"

SELECT *
  FROM {{ ref('dim_fin') }}
 WHERE dimension = 'ARY'
 