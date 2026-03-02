CREATE OR REPLACE TABLE yfinance_pipeline_prod.stocks_dataset.stocks_monthly_return AS
WITH monthly_dates AS (
    SELECT
        symbol,
        CAST(DATE_TRUNC('month', date) AS DATE) AS month_date,
        MIN(date) AS first_day,
        MAX(date) AS last_day
    FROM yfinance_pipeline_prod.stocks_dataset.stocks_price_history_raw
    WHERE date >= '2000-01-01'
    GROUP BY symbol, DATE_TRUNC('month', date)
),

monthly_returns AS (
    SELECT
        m.symbol,
        m.month_date,
        first_day,
        last_day,
        f.close AS first_close_of_month,
        l.close AS last_close_of_month,
        (l.close - f.close) / f.close * 100 AS monthly_pct_change
    FROM monthly_dates m
    JOIN yfinance_pipeline_prod.stocks_dataset.stocks_price_history_raw f
        ON m.symbol = f.symbol
        AND m.first_day = f.date
    JOIN yfinance_pipeline_prod.stocks_dataset.stocks_price_history_raw l
        ON m.symbol = l.symbol
        AND m.last_day = l.date
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY month_date
            ORDER BY monthly_pct_change DESC
        ) AS gain_rank,

        ROW_NUMBER() OVER (
            PARTITION BY month_date
            ORDER BY monthly_pct_change ASC
        ) AS loss_rank
    FROM monthly_returns
)

SELECT
    *
FROM ranked
ORDER BY month_date, monthly_pct_change DESC;