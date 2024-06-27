/*
1. Pull TTM ncfo and liabilitiesc into date spine and compute liabilitiesc_over_ncfo_ttm
2. For each date, find the minima of these numbers over the following year.
*/

WITH max_dt AS
	(SELECT MAX(date) AS max_dt FROM {{ ref('fct_daily') }}),

daily_art_ratio AS
	(SELECT fd.key_daily,
			b.key_dim_fin,
			fd.ticker,
			fd.date,
			fd.mcap_daily,
			fd.mcap_perc_rank,
			fd.mcap_perc_rank_rev,
			art.ncfo AS ncfo_ttm,
			art.liabilitiesc AS liabilitiesc_ttm,
			art.liabilitiesc / art.ncfo AS ratio_ttm,  -- high is bad
			CASE
				-- rare credit balance for liabilities is great, ratio goes negative, fine
				WHEN art.liabilitiesc < 0 AND art.ncfo > 0 THEN ratio_ttm
				-- neg. cash flow with liabilities -- I want this ratio pos
				WHEN art.liabilitiesc > 0 AND art.ncfo < 0 THEN -ratio_ttm
				-- credit balance but neg. CF, let it stay pos
				WHEN art.liabilitiesc < 0 AND art.ncfo < 0 THEN ratio_ttm
				ELSE ratio_ttm
				END AS ratio_adj_ttm
	   FROM {{ ref('fct_daily') }} AS fd
	  INNER JOIN max_dt
	 	 ON TRUE
	  INNER JOIN {{ ref('brg_fct_daily_dim_fin_art') }} AS b
		 ON fd.key_daily = b.key_daily
	  INNER JOIN {{ ref('dim_fin_art') }} AS art
		 ON b.key_dim_fin = art.key_dim_fin
	  WHERE NOT fd.interpolated_sep_row
		AND art.ncfo IS NOT NULL
		  ),

agg AS
	(SELECT t1.key_daily,
			-- ftm = following twelve months
	 	    MIN(t2.ncfo_ttm) AS min_ncfo_ftm,
			MAX(t2.ncfo_ttm) AS max_ncfo_ftm,
			MIN(t2.liabilitiesc_ttm) AS min_liabilitiesc_ftm,
			MAX(t2.liabilitiesc_ttm) AS max_liabilitiesc_ftm,
			MIN(t2.ratio_ttm) AS min_ratio_ftm,
			MAX(t2.ratio_ttm) AS max_ratio_ftm,
			MIN(t2.ratio_adj_ttm) AS min_ratio_adj_ftm,
			MAX(t2.ratio_adj_ttm) AS max_ratio_adj_ftm
	   FROM daily_art_ratio AS t1
	  INNER JOIN daily_art_ratio AS t2
	 	 ON t1.ticker = t2.ticker
		AND t2.date > t1.date
		AND t2.date <= t1.date + INTERVAL '1 YEAR'
	  GROUP BY 1
	  	  )

SELECT t1.*,
	   t2.min_ncfo_ftm,
	   t2.max_ncfo_ftm,
	   t2.min_liabilitiesc_ftm,
	   t2.max_liabilitiesc_ftm,
	   t2.min_ratio_ftm,
	   t2.max_ratio_ftm,
	   t2.min_ratio_adj_ftm,
	   t2.max_ratio_adj_ftm,
  FROM daily_art_ratio AS t1
  LEFT JOIN agg AS t2
	ON t1.key_daily = t2.key_daily
 -- Rows must be a year old to aggregate over following year
 INNER JOIN max_dt ON TRUE
 WHERE t1.date < max_dt.max_dt - INTERVAL '1 YEAR'