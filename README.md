# 📈 MarketPulse — Automated NSE Market Intelligence Platform

> An end-to-end automated data engineering pipeline that ingests, transforms, and visualizes daily NSE Nifty 50 stock data — with zero manual intervention.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.2-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![dbt](https://img.shields.io/badge/dbt-1.12-orange)
![Grafana](https://img.shields.io/badge/Grafana-10.4-yellow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)

---

## 🏗️ Architecture

yfinance NSE API
↓
Apache Airflow DAG (triggers weekdays at 4 PM IST)
↓
Python ingestion script (fetch → validate → load)
↓
PostgreSQL — Bronze layer (raw OHLCV data)
↓
dbt models (Bronze → Silver → Gold)
↓
PostgreSQL — Gold layer (RSI, MACD, Bollinger Bands, sector metrics)
↓
Grafana Dashboard + Custom HTML Frontend
↓
Docker Compose (containerizes everything)



---

## ✨ Key Features

- **Fully automated** — Airflow DAG runs every weekday at 4 PM IST after NSE market close. No manual steps.
- **Medallion architecture** — Bronze (raw) → Silver (cleaned) → Gold (analytics-ready) using dbt
- **Technical indicators** — RSI (14-period), 20-day & 50-day moving averages, Bollinger Bands computed as dbt SQL models
- **Sector analytics** — Banking, IT, Pharma, Auto, FMCG, Energy performance comparison
- **Live dashboard** — Custom dark-theme HTML frontend embedding 5 Grafana panels
- **Production patterns** — Docker Compose, data quality tests, upsert logic, retry handling

---

## 📊 Dashboard Panels

| Panel | Description |
|---|---|
| RSI Signals Table | All 47 stocks with RSI, trend signal, MA20, MA50 |
| Sector Performance | Average daily return % by sector (bar chart) |
| Top 10 Gainers | Best performing stocks today |
| Top 10 Losers | Worst performing stocks today |
| Price History | Close price + MA20 + MA50 for any stock (1 year) |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data Source | yfinance (Yahoo Finance NSE API) |
| Orchestration | Apache Airflow 2.9.2 |
| Database | PostgreSQL 15 |
| Transformation | dbt-core 1.12 |
| Visualization | Grafana 10.4 |
| Frontend | Custom HTML/CSS dashboard |
| Containerization | Docker + Docker Compose |
| Language | Python 3.12, SQL |

---

## 🗂️ Project Structure

marketpulse/
├── dags/
│ └── marketpulse_dag.py # Airflow DAG (fetch → validate → load)
├── ingestion/
│ └── fetch_stocks.py # yfinance ingestion script
├── dbt/
│ └── marketpulse_dbt/
│ └── models/
│ ├── staging/ # stg_stock_prices.sql
│ ├── intermediate/ # int_daily_returns.sql
│ └── marts/ # mart_technical_indicators.sql
│ # mart_sector_performance.sql
├── scripts/
│ └── init_db.sql # PostgreSQL schema init
├── docker-compose.yml # Full stack (Airflow + PG + Grafana)
└── .env.example # Environment variable template


---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+
- Git

### 1. Clone the repo
```bash
git clone https://github.com/Shubham21012001/marketpulse.git
cd marketpulse
```

### 2. Set up environment
```bash
cp .env.example .env
# Edit .env with your credentials if needed
```

### 3. Start the stack
```bash
docker-compose up airflow-init
docker-compose up -d
```

### 4. Load historical data (first time only)
```bash
pip install yfinance pandas sqlalchemy psycopg2-binary
python ingestion/fetch_stocks.py
```

### 5. Run dbt models
```bash
cd dbt/marketpulse_dbt
dbt run
```

### 6. Access services
| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

### 7. Open dashboard
Open `marketpulse_dashboard.html` in Chrome.

---

## 📐 dbt Models
bronze_stock_prices ← raw OHLCV from yfinance
↓
stg_stock_prices ← cleaned, validated, typed
↓
int_daily_returns ← daily returns, MA20, MA50, stddev
↓
mart_technical_indicators ← RSI(14), Bollinger Bands, trend signals
mart_sector_performance ← sector-wise aggregations + rankings


---

## 📅 Airflow DAG

**Schedule:** `30 10 * * 1-5` (4:00 PM IST, Monday to Friday)

**Tasks:**
fetch_and_load → validate → log_completion


---

## 🙏 Data Source

Stock data fetched via [yfinance](https://github.com/ranaroussi/yfinance) — 
NSE-listed stocks use `.NS` suffix (e.g. `RELIANCE.NS`, `TCS.NS`).

---

*Built by Shivam Chaubey — B.Tech CSE, Jaypee University of Engineering and Technology*