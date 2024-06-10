WITH curve_pools AS (
    SELECT address, name, distinct_name, pool
    FROM (VALUES
        (0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72, 'Curve', 'Curve USDC-USDe', 'USDC-USDe')
        , (0xf36a4ba50c603204c3fc6d2da8b78a7b69cbc67d, 'Curve', 'Curve DAI-USDe', 'DAI-USDe')
        , (0xf55b0f6f2da5ffddb104b58a60f2862745960442, 'Curve', 'Curve crvUSD-USDe', 'crvUSD-USDe')
        , (0x5dc1bf6f1e983c0b21efb003c105133736fa0743, 'Curve', 'Curve FRAX-USDe', 'FRAX-USDe')
        , (0x1ab3d612ea7df26117554dddd379764ebce1a5ad, 'Curve', 'Curve mkUSD-USDe', 'mkUSD-USDe')
        , (0x670a72e6d22b0956c0d2573288f82dcc5d6e3a61, 'Curve', 'Curve GHO-USDe', 'GHO-USDe')
        ) AS temp_table (address, name, distinct_name, pool)
        )
        

, time_series AS (
    SELECT time
    FROM (
    unnest(sequence(CAST('2023-11-20 00:00' AS timestamp), CAST(NOW() AS timestamp), interval '1' hour)) AS s(time)
    )
    )
    
    , transfers AS (
SELECT 
    time
    ,contract_address AS token_address
    ,symbol
    ,SUM(value) AS value
FROM (
SELECT 
        date_trunc('hour', evt_block_time) AS time
        , t.contract_address
        , b.symbol
        , SUM(t.value / POWER(10, b.decimals)) AS value
        FROM erc20_ethereum.evt_Transfer t
        LEFT JOIN tokens.erc20 AS b ON b.contract_address = t.contract_address
        WHERE t.evt_block_time >= date('2023-11-20')
        AND t.contract_address IN (0x4c9edd5852cd905f086c759e8383e09bff1e68b3 -- USDe
                                    , 0x6B175474E89094C44Da98b954EedeAC495271d0F -- DAI
                                    , 0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E -- crvUSD
                                    , 0x853d955aCEf822Db058eb8505911ED77F175b99e -- FRAX
                                    ,0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28 -- mkUSD
                                    ,0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f -- GHO
                                    ,0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 -- USDC
                                    )
        AND t.to IN (SELECT address FROM curve_pools)
        GROUP BY 1,2,3
        
        UNION ALL
        
        SELECT 
        date_trunc('hour', evt_block_time) AS time
        , t.contract_address
        , b.symbol
        , - SUM(t.value / POWER(10, b.decimals)) AS value
        FROM erc20_ethereum.evt_Transfer t
        LEFT JOIN tokens.erc20 AS b ON b.contract_address = t.contract_address
        WHERE t.evt_block_time >= date('2023-11-20')
        AND t.contract_address IN (0x4c9edd5852cd905f086c759e8383e09bff1e68b3 -- USDe
                                    , 0x6B175474E89094C44Da98b954EedeAC495271d0F -- DAI
                                    , 0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E -- crvUSD
                                    , 0x853d955aCEf822Db058eb8505911ED77F175b99e -- FRAX
                                    ,0x4591DBfF62656E7859Afe5e45f6f47D3669fBB28 -- mkUSD
                                    ,0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f -- GHO
                                    ,0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 -- USDC
                                    )
        AND t."from" IN (SELECT address FROM curve_pools)
        GROUP BY 1,2,3
        )
        GROUP BY 1,2,3
    )
    
, distinct_tokens AS (
    SELECT DISTINCT token_address, symbol
    FROM transfers
),

all_times_tokens AS (
    SELECT 
        ts.time, 
        dt.token_address, 
        dt.symbol
    FROM time_series ts
    CROSS JOIN distinct_tokens dt
),

backfilled_transfers AS (
    SELECT 
        att.time, 
        att.token_address, 
        att.symbol, 
        COALESCE(t.value, 0) AS value
    FROM all_times_tokens att
    LEFT JOIN transfers t
    ON att.time = t.time AND att.token_address = t.token_address
)

, data AS (
SELECT 
    time, 
    token_address AS token_address, 
    symbol, 
    SUM(SUM(value)) OVER (PARTITION BY token_address ORDER BY time) AS tvl
FROM backfilled_transfers
GROUP BY 1, 2, 3
ORDER BY 1, 2)

SELECT
    time
    ,symbol
    ,SUM(tvl) AS tvl 
    FROM (
SELECT 
    time
    ,CASE WHEN symbol = 'USDe' THEN 'USDe' ELSE 'Paired Stablecoin' END AS symbol
    ,tvl
FROM data)
GROUP BY 1,2
ORDER BY 1, 3 DESC


 
    
  
        
