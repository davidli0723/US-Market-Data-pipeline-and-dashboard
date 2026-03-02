CREATE OR REPLACE TABLE yfinance_pipeline_dev.stocks_dataset.stocks_annual_return AS
WITH yearly_dates AS (
    SELECT
        symbol,
        YEAR(date) AS year,
        MIN(date) AS first_day,
        MAX(date) AS last_day
    FROM yfinance_pipeline_dev.stocks_dataset.stocks_price_history_raw
    WHERE date >= '2000-01-01'
    GROUP BY symbol, YEAR(date)
),

yearly_returns AS (
    SELECT
        m.symbol,
        m.year,
        m.first_day,
        m.last_day,
        f.close AS first_close_of_year,
        l.close AS last_close_of_year,
        (l.close - f.close) / f.close * 100 AS yearly_pct_change
    FROM yearly_dates m
    JOIN yfinance_pipeline_dev.stocks_dataset.stocks_price_history_raw f
        ON m.symbol = f.symbol
        AND m.first_day = f.date
    JOIN yfinance_pipeline_dev.stocks_dataset.stocks_price_history_raw l
        ON m.symbol = l.symbol
        AND m.last_day = l.date
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY year
            ORDER BY yearly_pct_change DESC
        ) AS gain_rank,

        ROW_NUMBER() OVER (
            PARTITION BY year
            ORDER BY yearly_pct_change ASC
        ) AS loss_rank
    FROM yearly_returns
)

SELECT *
FROM ranked
ORDER BY year, yearly_pct_change DESC;