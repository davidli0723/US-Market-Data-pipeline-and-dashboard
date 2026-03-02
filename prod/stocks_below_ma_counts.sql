CREATE OR REPLACE TABLE yfinance_pipeline_prod.stocks_dataset.stocks_below_ma_counts AS
with LatestPrices as (
SELECT 
    symbol,
    date,
    close,
    ma_50,
    ma_250,
    -- Get the most recent record for each stock symbol
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
FROM yfinance_pipeline_prod.stocks_dataset.stocks_price_history_with_ma
)
SELECT
    lp.date,
    -- Count stocks below the 50-day MA
    SUM(CASE WHEN lp.close < lp.ma_50 THEN 1 ELSE 0 END) AS count_below_ma50,
    -- Count stocks below the 250-day MA
    SUM(CASE WHEN lp.close < lp.ma_250 THEN 1 ELSE 0 END) AS count_below_ma250,
    -- Count stocks below the 50-day and 250-day MA
    SUM(CASE WHEN (lp.close < lp.ma_50 or lp.close < lp.ma_250) THEN 1 ELSE 0 END) AS count_below_ma50_and_ma250,
    -- Total tech stocks tracked that day (for context)
    COUNT(lp.symbol) AS total_stocks
FROM LatestPrices lp
WHERE lp.rn <= 500
GROUP BY lp.date
ORDER BY lp.date DESC;