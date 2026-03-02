from pytickersymbols import PyTickerSymbols
import yfinance as yf
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType, StringType, BooleanType
import pandas as pd

def load_stocks_price(
    collected_symbols,
    table_name,
    fetch_period,
    batch_size=100
):
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])

    # 3. Processing in loops
    tickers = yf.Tickers(collected_symbols)
    # Initialize the Tickers object for this batch
    dfs = []
    # Example: Accessing data for each ticker in the current batch
    for symbol in collected_symbols:
        try:
            # Note: .info is slow. If you only need prices, use .history instead.
            # info_data = tickers_obj.tickers[symbol].info

            hist = tickers.tickers[symbol].history(period=fetch_period).reset_index()
            hist['symbol'] = symbol
            hist = hist[['symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            hist.columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            hist['date'] = hist['date'].dt.date
            dfs.append(hist)

        except Exception as e:
            print(f"Error fetching {symbol}: {e}")

    stocks_hist = pd.concat(dfs, ignore_index=True)


    stocks_hist_spark = spark.createDataFrame(stocks_hist, schema=schema)

    # Create or replace table stocks_price_history
    stocks_hist_spark.write.mode("append").saveAsTable(table_name)

    return 

def load_stocks_info(
    collected_symbols,
    table_name,
    batch_size=100
):
    stock_info_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("country", StringType(), True),
        StructField("shortName", StringType(), True),
        StructField("longName", StringType(), True),
        StructField("inDowJones", BooleanType(), True),
        # StructField("inNasDaq100", BooleanType(), True),
        # StructField("inS&P500", BooleanType(), True),
        # StructField("inS&P600", BooleanType(), True),
        StructField("industry", StringType(), True),
        StructField("sector", StringType(), True),
        StructField("marketCap", LongType(), True)
    ])

    for i in range(0, len(collected_symbols), batch_size):
        current_batch = collected_symbols[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}: {current_batch[0]} to {current_batch[-1]}...")

        tickers_obj = yf.Tickers(current_batch)
        info_rows = []
        for symbol in current_batch:
            try:
                info = tickers_obj.tickers[symbol].info
                row = {
                    "symbol": symbol,
                    "displayName": info.get("displayName"),
                    "country": info.get("country"),
                    "shortName": info.get("shortName"),
                    "longName": info.get("longName"),
                    "inDowJones": True if symbol in dowjones_symbols else False,
                    # "inNasDaq100": True if symbol in nasdaq100_symbols else False,
                    # "inS&P500": True if symbol in sp500_symbols else False,
                    # "inS&P600": True if symbol in sp600_symbols else False,
                    "industry": info.get("industry"),
                    "sector": info.get("sector"),
                    "marketCap": info.get("marketCap")
                }
                info_rows.append(row)
            except Exception as e:
                print(f"Error fetching info for {symbol}: {e}")

        info_df = pd.DataFrame(info_rows)
        info_spark_df = spark.createDataFrame(info_df, schema=stock_info_schema)
        info_spark_df.write.mode("append").saveAsTable("stocks_info")
    return 

def load_stocks_financial_statement(
    collected_symbols,
    balancesheet_table="yfinance_pipeline_dev.stocks_dataset.balance_sheet",
    income_table="yfinance_pipeline_dev.stocks_dataset.income_statement",
    cashflow_table="yfinance_pipeline_dev.stocks_dataset.cash_flow",
    batch_size=100
):
    for i in range(0, len(collected_symbols), batch_size):

        current_batch = collected_symbols[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}: {current_batch[0]} to {current_batch[-1]}...")

        tickers = yf.Tickers(current_batch)

        bs_dfs = []
        is_dfs = []
        cf_dfs = []

        for symbol in current_batch:
            try:
                ticker_obj = tickers.tickers[symbol]

                statements = {
                    "balance_sheet": ticker_obj.get_balance_sheet(freq="quarterly"),
                    "income_statement": ticker_obj.get_income_stmt(freq="quarterly"),
                    "cash_flow": ticker_obj.get_cashflow(freq="quarterly")
                }

                for statement_type, df in statements.items():

                    if df is None or df.empty:
                        continue

                    df_reset = df.reset_index().rename(columns={"index": "metrics"})

                    df_long = df_reset.melt(
                        id_vars=["metrics"],
                        var_name="date",
                        value_name="value"
                    )

                    df_long["symbol"] = symbol
                    df_long["date"] = pd.to_datetime(df_long["date"]).dt.date
                    df_long["value"] = df_long["value"].fillna(0)

                    df_long = df_long[["symbol", "metrics", "date", "value"]]

                    if statement_type == "balance_sheet":
                        bs_dfs.append(df_long)

                    elif statement_type == "income_statement":
                        is_dfs.append(df_long)

                    elif statement_type == "cash_flow":
                        cf_dfs.append(df_long)

            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

        # Write each table per batch

        if bs_dfs:
            bs_batch = spark.createDataFrame(pd.concat(bs_dfs, ignore_index=True))
            bs_batch.write.format("delta").mode("overwrite").saveAsTable(balancesheet_table)

        if is_dfs:
            is_batch = spark.createDataFrame(pd.concat(is_dfs, ignore_index=True))
            is_batch.write.format("delta").mode("overwrite").saveAsTable(income_table)

        if cf_dfs:
            cf_batch = spark.createDataFrame(pd.concat(cf_dfs, ignore_index=True))
            cf_batch.write.format("delta").mode("overwrite").saveAsTable(cashflow_table)

        print(f"Finished batch {i//batch_size + 1}")
    return 
