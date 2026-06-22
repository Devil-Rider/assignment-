-- =====================================================================================
-- TO Network FINAL  -- one row per (store_id, product_variant_id)
-- =====================================================================================
-- Collapses the RCA table (gold.scratch.to_network_allocation_rca, produced by
-- sql/to_network_allocation.sql) to a single winning MH per store x sku.
--
-- "final_mh_id" = the MH that supplied the most quantity for that store x sku.
-- Tie-break order : allocated-before-unallocated, then largest final_alloc,
--                   then highest priority tier, then best line-haul preference, then mh_id.
-- Store x sku with no allocation still produce exactly one row (final_mh_id = NULL),
-- with the reason carried through so it stays diagnosable.
-- =====================================================================================

WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY store_id, product_variant_id
            ORDER BY
                CASE WHEN final_alloc > 0 THEN 0 ELSE 1 END,  -- allocated rows win
                final_alloc        DESC,                      -- biggest supplier first
                priority_tier      ASC,                       -- then highest priority tier
                mh_pref_rank       ASC,                       -- then best line-haul preference
                mh_id                                          -- deterministic tie-break
        ) AS rn
    FROM gold.scratch.to_network_allocation_rca
)
SELECT
    store_id,
    product_variant_id,
    CASE WHEN final_alloc > 0 THEN mh_id         END AS final_mh_id,
    CASE WHEN final_alloc > 0 THEN mh_type       END AS final_mh_type,
    CASE WHEN final_alloc > 0 THEN expiry_bucket END AS final_expiry_bucket,
    CASE WHEN final_alloc > 0 THEN mh_pref_rank  END AS final_mh_pref_rank,
    coalesce(CASE WHEN final_alloc > 0 THEN final_alloc END, 0) AS final_mh_alloc_qty,
    store_total_allocated,          -- total qty allocated across ALL mhs for this store x sku
    requirement,
    store_unmet,
    base_validation_status,
    reason,
    run_date
FROM ranked
WHERE rn = 1;
