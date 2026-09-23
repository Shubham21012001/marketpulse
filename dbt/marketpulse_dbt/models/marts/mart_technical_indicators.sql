WITH base AS (
    SELECT * FROM {{ ref('int_daily_returns') }}
),

gains_losses AS (
    SELECT
        *,
        CASE WHEN daily_return_pct > 0
             THEN daily_return_pct ELSE 0 END AS gain,
        CASE WHEN daily_return_pct < 0
             THEN ABS(daily_return_pct) ELSE 0 END AS loss
    FROM base
),

rsi_calc AS (
    SELECT
        *,
        AVG(gain) OVER (
            PARTITION BY symbol ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain_14,
        AVG(loss) OVER (
            PARTITION BY symbol ORDER BY date
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss_14
    FROM gains_losses
)

SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    daily_return_pct,
    ma_20,
    ma_50,
    vol_ma_20,

    -- Bollinger Bands
    ROUND(ma_20 + (2 * stddev_20), 2) AS bb_upper,
    ROUND(ma_20 - (2 * stddev_20), 2) AS bb_lower,

    -- RSI (14-period)
    CASE
        WHEN avg_loss_14  = 0 THEN 100
        WHEN avg_gain_14  = 0 THEN 0
        ELSE ROUND(100 - (100 / (1 + (avg_gain_14 / avg_loss_14))), 2)
    END AS rsi_14,

    -- RSI signal
    CASE
        WHEN avg_loss_14 = 0                          THEN 'Overbought'
        WHEN avg_gain_14 = 0                          THEN 'Oversold'
        WHEN (100-(100/(1+(avg_gain_14/avg_loss_14)))) > 70 THEN 'Overbought'
        WHEN (100-(100/(1+(avg_gain_14/avg_loss_14)))) < 30 THEN 'Oversold'
        ELSE 'Neutral'
    END AS rsi_signal,

    -- Trend signal (price vs moving averages)
    CASE
        WHEN close > ma_20 AND ma_20 > ma_50 THEN 'Bullish'
        WHEN close < ma_20 AND ma_20 < ma_50 THEN 'Bearish'
        ELSE 'Neutral'
    END AS trend_signal,

    -- Distance from 52-week high/low proxy (vs MA50)
    ROUND(((close - ma_50) / NULLIF(ma_50, 0)) * 100, 2) AS pct_from_ma50

FROM rsi_calc
