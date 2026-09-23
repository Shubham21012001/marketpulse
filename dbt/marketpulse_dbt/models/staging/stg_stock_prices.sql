SELECT
    date::date          AS date,
    symbol,
    open::numeric       AS open,
    high::numeric       AS high,
    low::numeric        AS low,
    close::numeric      AS close,
    volume::bigint      AS volume
FROM bronze_stock_prices
WHERE close > 0
  AND high  >= low
  AND date  IS NOT NULL
  AND symbol IS NOT NULL