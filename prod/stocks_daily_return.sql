CREATE OR REPLACE TABLE yfinance_pipeline_prod.stocks_dataset.stocks_daily_return AS
WITH base AS (
  SELECT
    symbol,
    date,
    close,
    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
      / LAG(close) OVER (PARTITION BY symbol ORDER BY date) * 100
      AS pct_change_close_1d
  FROM yfinance_pipeline_prod.stocks_dataset.stocks_price_history_raw
  WHERE date >= '2020-01-01'
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY date
        ORDER BY pct_change_close_1d DESC
    ) AS gain_rank,
    ROW_NUMBER() OVER (
        PARTITION BY date
        ORDER BY pct_change_close_1d ASC
    ) AS loss_rank
  FROM base
  WHERE pct_change_close_1d IS NOT NULL
)
SELECT *
FROM ranked
ORDER BY date, pct_change_close_1d DESC;