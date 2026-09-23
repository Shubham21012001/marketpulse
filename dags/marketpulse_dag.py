from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/ingestion")

default_args = {
    "owner"           : "shivam",
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

def task_fetch_and_load():
    import os
    os.environ["CURL_CA_BUNDLE"] = ""
    os.environ["REQUESTS_CA_BUNDLE"] = ""

    import yfinance as yf
    import pandas as pd
    from sqlalchemy import create_engine, text
    from datetime import datetime, timedelta
    from curl_cffi import requests as cffi_requests

    DB_URL = "postgresql://airflow:airflow@postgres:5432/marketpulse"

    NIFTY_50 = [
        "HDFCBANK.NS", "ICICIBANK.NS", "KOTAKBANK.NS", "SBIN.NS",
        "AXISBANK.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
        "TCS.NS", "INFY.NS", "HCLTECH.NS", "WIPRO.NS", "TECHM.NS",
        "RELIANCE.NS", "ONGC.NS", "BPCL.NS", "COALINDIA.NS",
        "NTPC.NS", "POWERGRID.NS", "ADANIPORTS.NS",
        "TATASTEEL.NS", "JSWSTEEL.NS", "HINDALCO.NS",
        "HINDUNILVR.NS", "ITC.NS", "NESTLEIND.NS", "BRITANNIA.NS",
        "TATACONSUM.NS", "ASIANPAINT.NS",
        "MARUTI.NS", "HEROMOTOCO.NS", "EICHERMOT.NS",
        "SUNPHARMA.NS", "DRREDDY.NS", "CIPLA.NS", "DIVISLAB.NS",
        "LT.NS", "ULTRACEMCO.NS", "GRASIM.NS", "TITAN.NS",
        "BHARTIARTL.NS", "INDUSINDBK.NS", "SBILIFE.NS", "HDFCLIFE.NS",
        "ADANIENT.NS", "VEDL.NS", "M&M.NS", "UPL.NS",
        "BAJAJ-AUTO.NS", "SHRIRAMFIN.NS",
    ]

    engine = create_engine(DB_URL)
    end_date   = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")

    print(f"Fetching {start_date} to {end_date} using curl_cffi session...")

    session = cffi_requests.Session(impersonate="chrome110")

    records = []
    for ticker in NIFTY_50:
        try:
            t  = yf.Ticker(ticker, session=session)
            df = t.history(start=start_date, end=end_date, auto_adjust=True)
            if df.empty:
                print(f"  ⚠️  No data for {ticker}")
                continue
            df = df.reset_index()
            df["symbol"] = ticker.replace(".NS", "")
            df.rename(columns={
                "Date"  : "date",
                "Open"  : "open",
                "High"  : "high",
                "Low"   : "low",
                "Close" : "close",
                "Volume": "volume",
            }, inplace=True)
            df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]
            records.append(df)
            print(f"  ✅ {ticker}: {len(df)} rows")
        except Exception as e:
            print(f"  ⚠️  Skipping {ticker}: {e}")

    if not records:
        raise ValueError("No data fetched!")

    combined = pd.concat(records, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date

    combined["date"] = combined["date"].astype(str)

    with engine.begin() as conn:
        for _, row in combined.iterrows():
            conn.execute(text("""
                INSERT INTO bronze_stock_prices
                    (date, symbol, open, high, low, close, volume)
                VALUES
                    (:date, :symbol, :open, :high, :low, :close, :volume)
                ON CONFLICT (date, symbol) DO NOTHING
            """), {
                "date"  : str(row["date"]),
                "symbol": str(row["symbol"]),
                "open"  : float(row["open"]),
                "high"  : float(row["high"]),
                "low"   : float(row["low"]),
                "close" : float(row["close"]),
                "volume": int(row["volume"]),
            })

    print(f"✅ Loaded {len(combined)} rows for {combined['symbol'].nunique()} stocks")


def task_validate():
    from sqlalchemy import create_engine
    import pandas as pd

    DB_URL = "postgresql://airflow:airflow@postgres:5432/marketpulse"
    engine = create_engine(DB_URL)

    total = pd.read_sql(
        "SELECT COUNT(*) FROM bronze_stock_prices", engine
    ).iloc[0, 0]

    today_count = pd.read_sql(
        "SELECT COUNT(*) FROM bronze_stock_prices WHERE date = CURRENT_DATE",
        engine
    ).iloc[0, 0]

    print(f"✅ Total rows in bronze: {total}")
    print(f"✅ Today's rows: {today_count}")

    if today_count == 0:
        print("⚠️  No data for today — could be a market holiday or weekend")


def task_log_run():
    from datetime import datetime
    print(f"✅ MarketPulse pipeline completed at {datetime.now()}")
    print("Next run: tomorrow at 4 PM IST")


with DAG(
    dag_id            = "marketpulse_daily",
    default_args      = default_args,
    description       = "Daily NSE Nifty 50 stock data ingestion",
    schedule          = "30 10 * * 1-5",
    start_date        = datetime(2026, 9, 1),
    catchup           = False,
    tags              = ["marketpulse", "nse", "stocks"],
) as dag:

    fetch = PythonOperator(
        task_id         = "fetch_and_load",
        python_callable = task_fetch_and_load,
    )

    validate = PythonOperator(
        task_id         = "validate",
        python_callable = task_validate,
    )

    log_run = PythonOperator(
        task_id         = "log_completion",
        python_callable = task_log_run,
    )

    fetch >> validate >> log_run