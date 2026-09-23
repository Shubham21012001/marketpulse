WITH indicators AS (
    SELECT * FROM {{ ref('mart_technical_indicators') }}
),

sector_map (symbol, sector) AS (
    VALUES
        ('HDFCBANK',   'Banking'),
        ('ICICIBANK',  'Banking'),
        ('KOTAKBANK',  'Banking'),
        ('SBIN',       'Banking'),
        ('AXISBANK',   'Banking'),
        ('INDUSINDBK', 'Banking'),
        ('BAJFINANCE', 'Finance'),
        ('BAJAJFINSV', 'Finance'),
        ('SHRIRAMFIN', 'Finance'),
        ('TCS',        'IT'),
        ('INFY',       'IT'),
        ('HCLTECH',    'IT'),
        ('WIPRO',      'IT'),
        ('TECHM',      'IT'),
        ('RELIANCE',   'Energy'),
        ('ONGC',       'Energy'),
        ('BPCL',       'Energy'),
        ('COALINDIA',  'Energy'),
        ('NTPC',       'Energy'),
        ('POWERGRID',  'Energy'),
        ('TATASTEEL',  'Metals'),
        ('JSWSTEEL',   'Metals'),
        ('HINDALCO',   'Metals'),
        ('VEDL',       'Metals'),
        ('HINDUNILVR', 'FMCG'),
        ('ITC',        'FMCG'),
        ('NESTLEIND',  'FMCG'),
        ('BRITANNIA',  'FMCG'),
        ('TATACONSUM', 'FMCG'),
        ('MARUTI',     'Auto'),
        ('HEROMOTOCO', 'Auto'),
        ('EICHERMOT',  'Auto'),
        ('M&M',        'Auto'),
        ('BAJAJ-AUTO', 'Auto'),
        ('SUNPHARMA',  'Pharma'),
        ('DRREDDY',    'Pharma'),
        ('CIPLA',      'Pharma'),
        ('DIVISLAB',   'Pharma'),
        ('LT',         'Infrastructure'),
        ('ADANIPORTS', 'Infrastructure'),
        ('ADANIENT',   'Infrastructure'),
        ('ULTRACEMCO', 'Cement'),
        ('GRASIM',     'Cement'),
        ('ASIANPAINT', 'Consumer'),
        ('TITAN',      'Consumer'),
        ('BHARTIARTL', 'Telecom'),
        ('SBILIFE',    'Insurance'),
        ('HDFCLIFE',   'Insurance'),
        ('UPL',        'Chemicals')
)

SELECT
    i.date,
    i.symbol,
    sm.sector,
    i.close,
    i.daily_return_pct,
    i.rsi_14,
    i.rsi_signal,
    i.trend_signal,
    i.ma_20,
    i.ma_50,
    i.bb_upper,
    i.bb_lower,

    -- Sector average return for that day
    ROUND(AVG(i.daily_return_pct) OVER (
        PARTITION BY sm.sector, i.date
    ), 4) AS sector_avg_return,

    -- Sector rank by daily return
    RANK() OVER (
        PARTITION BY i.date
        ORDER BY i.daily_return_pct DESC NULLS LAST
    ) AS daily_return_rank

FROM indicators i
LEFT JOIN sector_map sm ON i.symbol = sm.symbol