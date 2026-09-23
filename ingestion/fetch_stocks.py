import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────
DB_URL = "postgresql://airflow:airflow@localhost:5432/marketpulse"

NIFTY_50 = [
    "HDFCBANK.NS", "ICICIBANK.NS", "KOTAKBANK.NS", "SBIN.NS",
    "AXISBANK.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS",
    "TCS.NS", "INFY.NS", "HCLTECH.NS", "WIPRO.NS", "TECHM.NS",
    "RELIANCE.NS", "ONGC.NS", "BPCL.NS", "COALINDIA.NS",
    "NTPC.NS", "POWERGRID.NS", "ADANIPORTS.NS",
    "TATASTEEL.NS", "JSWSTEEL.NS", "HINDALCO.NS",
    "HINDUNILVR.NS", "ITC.NS", "NESTLEIND.NS", "BRITANNIA.NS",
    "TATACONSUM.NS", "ASIANPAINT.NS",
    "MARUTI.NS", "TATAMOTORS.NS", "HEROMOTOCO.NS", "EICHERMOT.NS",
    "SUNPHARMA.NS", "DRREDDY.NS", "CIPLA.NS", "DIVISLAB.NS",
    "LT.NS", "ULTRACEMCO.NS", "GRASIM.NS", "TITAN.NS",
    "BHARTIARTL.NS", "INDUSINDBK.NS", "SBILIFE.NS", "HDFCLIFE.NS",
    "ADANIENT.NS", "VEDL.NS", "M&M.NS", "UPL.NS",
    "BAJAJ-AUTO.NS", "SHRIRAMFIN.NS",
]

# ── Step 1: Create bronze table ───────────────────────────────
def create_bronze_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze_stock_prices (
                id         SERIAL PRIMARY KEY,
                date       DATE        NOT NULL,
                symbol     VARCHAR(20) NOT NULL,
                open       NUMERIC(12,4),
                high       NUMERIC(12,4),
                low        NUMERIC(12,4),
                close      NUMERIC(12,4),
                volume     BIGINT,
                loaded_at  TIMESTAMP DEFAULT NOW(),
                UNIQUE (date, symbol)
            );
        """))
        conn.commit()
    print("✅ Bronze table ready")

# ── Step 2: Fetch from yfinance ───────────────────────────────
def fetch_stocks(days_back=365):
    end_date   = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    print(f"Fetching {len(NIFTY_50)} stocks from {start_date} to {end_date}...")

    raw = yf.download(
        tickers     = NIFTY_50,
        start       = start_date,
        end         = end_date,
        interval    = "1d",
        group_by    = "ticker",
        auto_adjust = True,
        threads     = True,
        progress    = False,
    )

    records = []
    for ticker in NIFTY_50:
        try:
            df = raw[ticker].dropna().reset_index()
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
        except Exception as e:
            print(f"  ⚠️  Skipping {ticker}: {e}")

    combined = pd.concat(records, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    print(f"✅ Fetched {len(combined)} rows for {combined['symbol'].nunique()} stocks")
    return combined

# ── Step 3: Validate ──────────────────────────────────────────
def validate(df):
    issues = []
    if df.empty:
        issues.append("Empty dataframe")
    if (df[["open","high","low","close"]] <= 0).any().any():
        issues.append("Negative or zero prices found")
    if (df["high"] < df["low"]).any():
        issues.append("high < low in some rows")
    if df[["date","symbol","close"]].isnull().any().any():
        issues.append("Nulls in critical columns")

    if issues:
        for i in issues:
            print(f"  ❌ {i}")
        return False

    print(f"✅ Validation passed — {len(df)} rows, {df['symbol'].nunique()} stocks")
    return True

# ── Step 4: Load to PostgreSQL ────────────────────────────────
def load_to_postgres(df, engine):
    df.to_sql(
        name      = "bronze_stock_prices",
        con       = engine,
        if_exists = "append",
        index     = False,
        method    = "multi",
    )
    count = pd.read_sql(
        "SELECT COUNT(*) FROM bronze_stock_prices", engine
    ).iloc[0, 0]
    print(f"✅ Bronze table now has {count} total rows")

# ── Main ──────────────────────────────────────────────────────
def run(days_back=365):
    print("── MarketPulse ingestion starting ──")
    engine = create_engine(DB_URL)

    create_bronze_table(engine)
    df = fetch_stocks(days_back)

    if not validate(df):
        raise ValueError("Validation failed — stopping")

    load_to_postgres(df, engine)
    print("── Done ✅ ──")

if __name__ == "__main__":
    run(days_back=365)   # loads 1 year of history on first run