-- ============================================================================
-- LATAM Stablecoins - Flows with Mint/Burn Detection (Optimized)
-- ============================================================================
-- Cost: ~10-15 credits
-- Output: ~100-250 rows (weekly aggregates)
-- Execution: <10 seconds
-- Added: Mint/Burn detection for supply verification
-- Fixed: varbinary type casting for address comparison
-- ============================================================================

WITH token_list AS (
    SELECT 'bnb' as blockchain, 0xb6bb22f4d1e58e9e43efa2ec7f572d215b3cf08a as contract_address, 'BBRL' as symbol, 18 as decimals
    UNION ALL
    SELECT 'polygon', 0x5c067c80c00ecd2345b05e83a3e758ef799c40b5, 'BRL1', 18
    UNION ALL
    SELECT 'celo', 0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'BRLA', 18
    UNION ALL
    SELECT 'polygon', 0xe6a537a407488807f0bbeb0038b79004f19dddfb, 'BRLA', 18
    UNION ALL
    SELECT 'gnosis', 0xfecb3f7c54e2caae9dc6ac9060a822d47e053760, 'BRLA', 18
    UNION ALL
    SELECT 'gnosis', 0x0a06c8354a6cc1a07549a38701eac205942e3ac6, 'BRZ', 18
    UNION ALL
    SELECT 'polygon', 0x4ed141110f6eeeaba9a1df36d8c26f684d2475dc, 'BRZ', 18
    UNION ALL
    SELECT 'base', 0xe9185ee218cae427af7b9764a011bb89fea761b4, 'BRZ', 18
    UNION ALL
    SELECT 'unichain', 0xe9185ee218cae427af7b9764a011bb89fea761b4, 'BRZ', 18
    UNION ALL
    SELECT 'polygon', 0x12050c705152931cfee3dd56c52fb09dea816c23, 'COPM', 18
    UNION ALL
    SELECT 'base', 0x269cae7dc59803e5c596c95756faeebb6030e0af, 'MXNe', 6
    UNION ALL
    SELECT 'base', 0x5e40f26e89213660514c51fb61b2d357dbf63c85, 'nARS', 18
)

SELECT
    tl.blockchain,
    tl.symbol,
    DATE_TRUNC('week', t.block_time) as week_start,
    
    -- Weekly aggregates
    COUNT(*) as transfer_count,
    COUNT(DISTINCT t."from") as unique_senders,
    COUNT(DISTINCT t."to") as unique_receivers,
    SUM(t.amount) as total_amount_raw,
    SUM(t.amount / POWER(10, tl.decimals)) as total_amount_normalized,
    SUM(t.amount_usd) as total_volume_usd,
    AVG(t.amount_usd) as avg_transfer_usd,
    MIN(t.amount_usd) as min_transfer_usd,
    MAX(t.amount_usd) as max_transfer_usd,
    
    -- NEW: Mint/Burn Detection (for supply verification)
    -- FIXED: Use 0x0000... (varbinary literal) instead of '0x0000...' (varchar)
    COUNT(CASE WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN 1 END) as mint_count,
    SUM(CASE WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN t.amount_usd ELSE 0 END) as mint_volume_usd,
    SUM(CASE WHEN t."to" = 0x0000000000000000000000000000000000000000 THEN t.amount_usd ELSE 0 END) as burn_volume_usd

FROM tokens.transfers t
INNER JOIN token_list tl ON
    t.blockchain = tl.blockchain
    AND t.contract_address = tl.contract_address
WHERE t.block_date >= CURRENT_DATE - INTERVAL '21' DAY  -- 3 weeks for better WoW comparison
    AND t.block_date < CURRENT_DATE
    AND t.blockchain IN ('bnb', 'polygon', 'celo', 'gnosis', 'base', 'unichain')
GROUP BY
    tl.blockchain,
    tl.symbol,
    DATE_TRUNC('week', t.block_time)
ORDER BY
    week_start DESC,
    total_volume_usd DESC
