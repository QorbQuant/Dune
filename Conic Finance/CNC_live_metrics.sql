-- DUNE SQL

-- Credit to Oxboxer for price calc
-- WITH dex_trades AS (
--         SELECT 
--             token_bought_address as contract_address,  -- was token a
--             amount_usd/token_bought_amount as price,
--             block_time
--         FROM dex.trades
--         WHERE 1=1
--         AND amount_usd  > 0
--         --AND category = 'DEX'
--         AND token_bought_amount > 0
        
--         UNION ALL 
        
--         SELECT 
--             token_sold_address as contract_address, 
--             amount_usd/token_sold_amount as price,
--             block_time
--         FROM dex.trades
--         WHERE 1=1
--         AND amount_usd  > 0
--       --  AND category = 'DEX'
--         AND token_sold_amount > 0
        
--     ),
    
    
--     rawdata as (

--     SELECT 
--         date_trunc('hour', block_time) as hour,
--         d.contract_address,
--         e.symbol as asset,
--         (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)) AS price,
--         count(*) AS sample_size
--     FROM dex_trades d
--     left join tokens.erc20 AS e on e.contract_address = d.contract_address
--     GROUP BY 1, 2, 3

--     )

--     ,leaddata as 
--     (
--     SELECT
--     hour,
--     contract_address,
--     asset,
--     price,
--     sample_size,
--     lead(hour, 1, now() ) OVER (PARTITION BY contract_address ORDER BY hour asc) AS next_hour
--     from rawdata
--     where sample_size > 4
--     )
    
--     ,hours AS
--     (
--     SELECT
--     explode(sequence(to_date('2020-01-01'), date_trunc('hour', NOW()), interval '1 hour')) AS hour
--     )

WITH eth_price AS (
SELECT
   DATE_TRUNC('minute', minute) AS time
    ,AVG(price) AS eth_price
FROM prices.usd
WHERE contract_address = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
AND minute > '2022-04-01'
GROUP BY 1
    )

, cnc_price AS (
SELECT 
    DATE_TRUNC('minute', evt_block_time) AS time
    ,tokens_sold/tokens_bought AS price
    ,tokens_bought AS amount
FROM curvefi_ethereum.cnc_eth_pool_evt_TokenExchange
WHERE sold_id = 0

UNION

SELECT
    DATE_TRUNC('minute', evt_block_time) AS time
    ,tokens_bought/tokens_sold AS price
    ,tokens_sold AS amount
FROM curvefi_ethereum.cnc_eth_pool_evt_TokenExchange
WHERE sold_id = 1 AND tokens_bought > 0)

, fin1 AS (
SELECT 
    time
    ,SUM(price * amount) /SUM(amount) AS price
FROM cnc_price
--WHERE amount > 1
GROUP BY 1
  )

, fin2 AS (
SELECT
    f1.time AS minute
    ,f1.price 
    ,f.eth_price
    ,f1.price * f.eth_price AS cnc_price
FROM fin1 AS f1
JOIN eth_price AS f
    ON f1.time = f.time
    )
    
, price AS (
SELECT 
    *
    ,cnc_price * 10000000 AS fdv
    ,((cnc_price - lag(cnc_price, 1440) over (order by minute ASC)) / lag(cnc_price, 1440) over (order by minute ASC)) AS day_change
    ,((cnc_price - lag(cnc_price, 10080) over (order by minute ASC)) / lag(cnc_price, 10080) over (order by minute ASC)) AS week_change
FROM fin2
ORDER BY 1 DESC)


-- , price AS (
--     SELECT
--     d.hour as hour,
--     contract_address,
--     asset,
--     price,
--     price * 10000000 AS fdv,
--     ((price - lag(price, 24) over (order by d.hour ASC)) / lag(price, 24) over (order by d.hour ASC)) AS day_change,
--     ((price - lag(price, 168) over (order by d.hour ASC)) / lag(price, 168) over (order by d.hour ASC)) AS week_change,
--     sample_size
--     from leaddata b
--     INNER JOIN hours d ON b.hour <= d.hour
--     AND d.hour < b.next_hour -- Yields an observation for every hour after the first transfer until the next hour with transfer
--     where contract_address = LOWER('0x9aE380F0272E2162340a5bB646c354271c0F5cFC')
--     ORDER BY d.hour DESC)
    
, supply AS (
SELECT
    DATE_TRUNC('minute', evt_block_time) AS minute
    ,SUM(value / POWER (10, 18)) AS deposits
FROM erc20_ethereum.evt_Transfer
WHERE contract_address = LOWER('0x9aE380F0272E2162340a5bB646c354271c0F5cFC') -- CNC token contract
    AND from = '0x0000000000000000000000000000000000000000'
GROUP BY 1
    )

, base_data AS (
    (with days AS 
            (SELECT explode(sequence(to_date('2022-01-01'), date_trunc('minute', NOW()), interval '1 minute')) AS minute)
        SELECT 
        minute
        ,0 AS total 
        FROM
        days
       )
    )

, over_time AS (
SELECT 
    t1.minute
    ,t1.total AS total_base
    ,t2.deposits
FROM base_data AS t1
LEFT JOIN supply AS t2 ON t2.minute = t1.minute
    )
, finish_supply AS (
SELECT 
    minute
    ,SUM(total_base + deposits) OVER (ORDER BY minute) AS circ_supply
FROM over_time
WHERE minute > '2022-04-1')

SELECT
    a.minute
    --,a.asset
    ,a.cnc_price
    ,a.fdv
    ,a.day_change * 100 AS day_change
    ,a.week_change
    ,a.cnc_price * b.circ_supply AS mcap
FROM price AS a
LEFT JOIN finish_supply AS b ON a.minute = b.minute
ORDER BY a.minute DESC
