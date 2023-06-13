-- DUNE SQL
--
WITH transfers AS (
SELECT 
     DATE_TRUNC('day', evt_block_time) AS day
     ,SUM(CAST(value AS DOUBLE) / POWER(10,18)) AS locked
FROM erc20_ethereum.evt_Transfer 
WHERE contract_address = 0x9aE380F0272E2162340a5bB646c354271c0F5cFC -- CNC Token
AND to IN (0x3F41480DD3b32F1cC579125F9570DCcD07E07667 -- locking contract v1
            ,0x5F2e1Ac047E6A8526f8640a7Ed8AB53a0b3f4acF) -- locking contract v2 
GROUP BY 1

UNION ALL

SELECT
    DATE_TRUNC('day', evt_block_time) AS day
     ,SUM(-CAST(value AS DOUBLE) /POWER(10,18)) 
FROM erc20_ethereum.evt_Transfer 
WHERE contract_address = 0x9aE380F0272E2162340a5bB646c354271c0F5cFC -- CNC Token
AND "from" IN (0x3F41480DD3b32F1cC579125F9570DCcD07E07667 -- locking contract v1
                ,0x5F2e1Ac047E6A8526f8640a7Ed8AB53a0b3f4acF) -- locking contract v2
GROUP BY 1
    )

SELECT 
    day
    ,locked
    ,SUM(locked) OVER (ORDER BY day ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7_day_amount
    ,SUM(locked) OVER (ORDER BY day ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30_day_amount
FROM (
    SELECT 
        DATE_TRUNC('day', day) AS day
        ,SUM(locked) AS locked
    FROM transfers
    WHERE day > CAST('2022-04-06' AS timestamp)
    GROUP BY 1
) subquery
ORDER BY 1 DESC



