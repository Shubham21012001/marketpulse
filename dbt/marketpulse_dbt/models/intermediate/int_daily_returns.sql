SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume,

    -- Daily % return
    ROUND(
        (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
        / NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0)
        * 100
    , 4) AS daily_return_pct,

    -- 20-day simple moving average
    ROUND(AVG(close) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 2) AS ma_20,

    -- 50-day simple moving average
    ROUND(AVG(close) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ), 2) AS ma_50,

    -- 20-day volume moving average
    ROUND(AVG(volume) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 0) AS vol_ma_20,

    -- 20-day standard deviation (for Bollinger Bands)
    ROUND(STDDEV(close) OVER (
        PARTITION BY symbol ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ), 4) AS stddev_20

FROM {{ ref('stg_stock_prices') }}