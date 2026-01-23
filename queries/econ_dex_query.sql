-- ============================================================================
-- LATAM Stablecoins - DEX Volume with Market Sentiment Analysis (Optimized)
-- ============================================================================
-- Cost: ~3-5 credits (vs 415-425 raw)
-- Output: ~250 rows (daily aggregates)
-- Execution: <5 seconds
-- Features: Buy/sell direction tracking, market sentiment, liquidity metrics
-- ============================================================================

WITH token_list AS (
    SELECT 'bnb' as blockchain, 0xb6bb22f4d1e58e9e43efa2ec7f572d215b3cf08a as contract_address, 'BBRL' as symbol
    UNION ALL
    SELECT 'polygon', 0x5c067c80c00ecd2345b05e83a3e758ef799c40b5, 'BRL1'
    UNION ALL
    SELECT 'celo', 0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'BRLA'
    UNION ALL
    SELECT 'polygon', 0xe6a537a407488807f0bbeb0038b79004f19dddfb, 'BRLA'
    UNION ALL
    SELECT 'gnosis', 0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'BRLA'
    UNION ALL
    SELECT 'gnosis', 0x0a06c8354a6cc1a07549a38701eac205942e3ac6, 'BRZ'
    UNION ALL
    SELECT 'polygon', 0x4ed141110f6eeeaba9a1df36d8c26f684d2475dc, 'BRZ'
    UNION ALL
    SELECT 'base', 0xe9185ee218cae427af7b9764a011bb89fea761b4, 'BRZ'
    UNION ALL
    SELECT 'unichain', 0xe9185ee218cae427af7b9764a011bb89fea761b4, 'BRZ'
    UNION ALL
    SELECT 'polygon', 0x12050c705152931cfee3dd56c52fb09dea816c23, 'COPM'
    UNION ALL
    SELECT 'base', 0x269cae7dc59803e5c596c95756faeebb6030e0af, 'MXNe'
    UNION ALL
    SELECT 'base', 0x5e40f26e89213660514c51fb61b2d357dbf63c85, 'nARS'
),

token_bought_trades AS (
    -- Trades where tracked token was BOUGHT (inflow/accumulation pressure)
    SELECT
        dt.blockchain,
        tl.symbol,
        dt.block_date,
        dt.project,
        'bought' as direction,
        dt.amount_usd
    FROM dex.trades dt
    INNER JOIN token_list tl ON
        dt.blockchain = tl.blockchain
        AND dt.token_bought_address = tl.contract_address
    WHERE dt.block_date >= CURRENT_DATE - INTERVAL '21' DAY  -- 3 weeks for WoW comparison
        AND dt.block_date < CURRENT_DATE
        AND dt.blockchain IN ('bnb', 'polygon', 'celo', 'gnosis', 'base', 'unichain')
        AND dt.amount_usd >= 1  -- Filter dust trades
),

token_sold_trades AS (
    -- Trades where tracked token was SOLD (outflow/distribution pressure)
    SELECT
        dt.blockchain,
        tl.symbol,
        dt.block_date,
        dt.project,
        'sold' as direction,
        dt.amount_usd
    FROM dex.trades dt
    INNER JOIN token_list tl ON
        dt.blockchain = tl.blockchain
        AND dt.token_sold_address = tl.contract_address
    WHERE dt.block_date >= CURRENT_DATE - INTERVAL '21' DAY  -- 3 weeks for WoW comparison
        AND dt.block_date < CURRENT_DATE
        AND dt.blockchain IN ('bnb', 'polygon', 'celo', 'gnosis', 'base', 'unichain')
        AND dt.amount_usd >= 1  -- Filter dust trades
),

all_trades AS (
    -- Combine both buy and sell directions
    SELECT * FROM token_bought_trades
    UNION ALL
    SELECT * FROM token_sold_trades
)

-- Daily aggregated summary with market sentiment
SELECT
    blockchain,
    symbol,
    block_date as date,
    
    -- Volume metrics
    COUNT(*) as trade_count,
    SUM(amount_usd) as total_volume_usd,
    AVG(amount_usd) as avg_trade_size_usd,
    MIN(amount_usd) as min_trade_usd,
    MAX(amount_usd) as max_trade_usd,
    
    -- Direction breakdown (market sentiment)
    SUM(CASE WHEN direction = 'bought' THEN amount_usd ELSE 0 END) as buy_volume_usd,
    SUM(CASE WHEN direction = 'sold' THEN amount_usd ELSE 0 END) as sell_volume_usd,
    SUM(CASE WHEN direction = 'bought' THEN 1 ELSE 0 END) as buy_count,
    SUM(CASE WHEN direction = 'sold' THEN 1 ELSE 0 END) as sell_count,
    
    -- Net flow indicator (positive = accumulation, negative = distribution)
    SUM(CASE WHEN direction = 'bought' THEN amount_usd ELSE -amount_usd END) as net_buy_pressure_usd,
    
    -- Market sentiment ratio (buy volume / total volume)
    ROUND(
        SUM(CASE WHEN direction = 'bought' THEN amount_usd ELSE 0 END) * 100.0 / 
        NULLIF(SUM(amount_usd), 0),
        2
    ) as buy_pressure_pct,
    
    -- Liquidity and distribution metrics
    APPROX_DISTINCT(project) as unique_dex_count

FROM all_trades
GROUP BY
    blockchain,
    symbol,
    block_date
ORDER BY
    date DESC,
    total_volume_usd DESC
