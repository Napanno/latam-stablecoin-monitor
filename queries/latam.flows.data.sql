-- ============================================================================
-- LATAM Stablecoins - Flows with Mint/Burn Detection (Optimized)
-- ============================================================================
-- Cost: ~10-15 credits
-- Output: ~100-250 rows (weekly aggregates)
-- Execution: <10 seconds
-- Added: Mint/Burn detection for supply verification
-- Fixed: varbinary type casting for address comparison
-- Added: burn_count
-- Removed: Low value columns
-- ============================================================================

SELECT
    tl.blockchain,
    tl.symbol,
    DATE_TRUNC('week', t.block_time) as week_start,
    
    -- Activity metrics
    COUNT(*) as transfer_count,
    COUNT(DISTINCT t."from") as unique_senders,
    COUNT(DISTINCT t."to") as unique_receivers,
    
    -- Volume metrics (USD for comparison)
    SUM(t.amount_usd) as total_volume_usd,
    AVG(t.amount_usd) as avg_transfer_usd,
    MAX(t.amount_usd) as max_transfer_usd,
    
    -- Native token amounts (optional for % calculations)
    SUM(t.amount / POWER(10, tl.decimals)) as total_amount_normalized,
    
    -- Mint/Burn detection
    COUNT(CASE WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN 1 END) as mint_count,
    SUM(CASE WHEN t."from" = 0x0000000000000000000000000000000000000000 THEN t.amount_usd ELSE 0 END) as mint_volume_usd,
    COUNT(CASE WHEN t."to" = 0x0000000000000000000000000000000000000000 THEN 1 END) as burn_count, -- ADDED
    SUM(CASE WHEN t."to" = 0x0000000000000000000000000000000000000000 THEN t.amount_usd ELSE 0 END) as burn_volume_usd
    
FROM tokens.transfers t
INNER JOIN token_list tl ON
    t.blockchain = tl.blockchain
    AND t.contract_address = tl.contract_address
WHERE t.block_date >= CURRENT_DATE - INTERVAL '21' DAY
    AND t.block_date < CURRENT_DATE
    AND t.blockchain IN ('bnb', 'polygon', 'celo', 'gnosis', 'base', 'unichain')
GROUP BY
    tl.blockchain,
    tl.symbol,
    DATE_TRUNC('week', t.block_time)
ORDER BY
    week_start DESC,
    total_volume_usd DESC
