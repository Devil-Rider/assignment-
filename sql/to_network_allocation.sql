-- =====================================================================================
-- TO Network Allocation  (greedy, SQL-first rebuild)
-- =====================================================================================
-- Goal : For every (store_id, product_variant_id) in the TO base, decide which MH(s)
--        supply it, by greedily consuming warehouse inventory in a fixed priority order,
--        capped by each store's requirement.
--
-- Design choices (kept deliberately simple / "no fancy things"):
--   * Pure Databricks SQL, CTE pipeline, easy to read top-to-bottom.
--   * NOTHING is filtered out of the final result. The output is an RCA table at
--     (store_id, product_variant_id, mh_id, expiry_bucket) grain. Every base row and
--     every candidate MH is kept, with a `reason` column + all running balances, so you
--     can filter to one store x sku and see exactly why a given MH was / wasn't picked.
--
-- Allocation priority (highest first):
--      1) expiry_in_30days  + dead   MH
--      2) expiry_in_30days  + buying MH
--      3) expiry_post_30days + dead   MH
--      4) expiry_post_30days + buying MH
--   Tie-break within a tier: MH with the highest inventory in that bucket goes first.
--
-- Greedy mechanics (2-pass cumsum):
--   Pass 1 (store waterfall) : a store's requirement is spread down its MH priority list
--                              via a cumulative sum of MH inventory  -> desired_alloc.
--                              This gives demand "spillover" across MHs.
--   Pass 2 (MH supply cap)   : per (mh, sku, bucket) the shared inventory pool is split
--                              across competing stores via a cumulative sum of
--                              desired_alloc, capped by the pool  -> final_alloc.
--                              This enforces "shared inventory".
--   NOTE: exact two-sided greedy (re-spilling a store that got cut short in pass 2)
--         requires iteration -> move to Python later. Both running balances are exposed
--         in the RCA so the single-shot approximation is fully transparent.
-- =====================================================================================

CREATE OR REPLACE TABLE gold.scratch.to_network_allocation_rca AS

WITH
-- --------------------------------------------------------------------------------------
-- 1. INVENTORY: warehouse stock split into two expiry buckets.
--    expiry_raw_data_summary.store_id  IS the warehouse / MH.
--    Split rule: (picking_date - today) <= 30  -> in_30 , else post_30.
-- --------------------------------------------------------------------------------------
inv_raw AS (
    SELECT
        store_id                                   AS mh_id,
        sku                                        AS product_variant_id,
        picking_date,
        available_qty,
        inv_bucket,
        datediff(picking_date, current_date())     AS days_to_picking,
        CASE WHEN datediff(picking_date, current_date()) <= 30
             THEN 'in_30' ELSE 'post_30' END       AS expiry_bucket
    FROM gold.ops.expiry_raw_data_summary
    WHERE lower(inv_bucket) IN ('good', 'deep')
      AND upper(store_type) = 'WAREHOUSE'
      AND available_qty IS NOT NULL
),

inv_by_bucket AS (              -- long form: one row per (mh, sku, expiry_bucket)
    SELECT
        mh_id,
        product_variant_id,
        expiry_bucket,
        sum(available_qty)      AS mh_inventory_qty
    FROM inv_raw
    GROUP BY mh_id, product_variant_id, expiry_bucket
    HAVING sum(available_qty) > 0
),

-- --------------------------------------------------------------------------------------
-- 2. BASE: the universe we must build network for.
-- --------------------------------------------------------------------------------------
to_base AS (
    SELECT DISTINCT
        store_id,
        product_variant_id
    FROM gold.planning.to_base_table
),

-- --------------------------------------------------------------------------------------
-- 3. PO VALIDATION (eligibility): an open PO means no network needed.
--    Keep store x sku where status is 'closed' or there is no/NULL status; drop 'open'.
--    NOT applied as a hard filter -> carried as a flag so open rows stay in the RCA.
-- --------------------------------------------------------------------------------------
po_status AS (
    SELECT
        store_id,
        product_variant_id,
        count(*)                                                              AS po_row_cnt,
        max(CASE WHEN lower(validation_status) = 'open'   THEN 1 ELSE 0 END)  AS has_open,
        max(CASE WHEN lower(validation_status) = 'closed' THEN 1 ELSE 0 END)  AS has_closed
    FROM gold.planning.po_base_table
    GROUP BY store_id, product_variant_id
),

-- --------------------------------------------------------------------------------------
-- 4. BUYING MH SET: (mh, sku) that are actively bought = closed PO on a live store.
--    Any candidate (mh, sku) in this set -> "buying" MH, otherwise -> "dead" MH.
-- --------------------------------------------------------------------------------------
buying_set AS (
    SELECT DISTINCT
        mh_id,
        product_variant_id
    FROM gold.planning.po_base_table
    WHERE lower(validation_status) = 'closed'
      AND lower(store_phase)       = 'live'
      AND mh_id IS NOT NULL
),

-- --------------------------------------------------------------------------------------
-- 5a. CANDIDATE MHs (primary source): store x product line-haul selection.
--     Unpivot the 4 preference slots; drop empty/null.
-- --------------------------------------------------------------------------------------
cand_a AS (
    SELECT store_id, product_variant_id, mh_pref_rank, mh_id, 'final_sec_mh_selection' AS mh_source
    FROM (
        SELECT
            store_id,
            product_variant_id,
            stack(4,
                  1, to_primary_mh,
                  2, to_secondary_mh,
                  3, to_tertiary_mh,
                  4, to_quaternary_mh
            ) AS (mh_pref_rank, mh_id)
        FROM gold.scratch.final_sec_mh_selection
    ) s
    WHERE nullif(trim(mh_id), '') IS NOT NULL
),

-- store x sku that already have at least one candidate from the primary source.
cand_a_keys AS (
    SELECT DISTINCT store_id, product_variant_id
    FROM cand_a
),

-- --------------------------------------------------------------------------------------
-- 5b. CANDIDATE MHs (fallback source): all MHs linked to the store in store_master_live.
--     Used only for store x sku NOT covered by the primary source (5a).
-- --------------------------------------------------------------------------------------
store_master_mh AS (
    SELECT DISTINCT store_id, mh_id
    FROM (
        SELECT
            store_id,
            stack(4,
                  to_primary_mh,
                  to_secondary_mh,
                  to_tertiary_mh,
                  to_quaternary_mh
            ) AS (mh_id)
        FROM gold.ops.store_master_live
    ) u
    WHERE nullif(trim(mh_id), '') IS NOT NULL
),

cand_b AS (
    SELECT
        b.store_id,
        b.product_variant_id,
        CAST(99 AS INT)            AS mh_pref_rank,     -- fallback, lowest preference
        m.mh_id,
        'store_master_live'        AS mh_source
    FROM to_base b
    LEFT ANTI JOIN cand_a_keys k
        ON  b.store_id           = k.store_id
        AND b.product_variant_id = k.product_variant_id
    JOIN store_master_mh m
        ON b.store_id = m.store_id
),

-- --------------------------------------------------------------------------------------
-- 5c. ALL CANDIDATE MHs (primary + fallback), de-duplicated to best preference.
-- --------------------------------------------------------------------------------------
candidate_mh AS (
    SELECT
        store_id,
        product_variant_id,
        mh_id,
        min(mh_pref_rank)                                                   AS mh_pref_rank,
        min_by(mh_source, mh_pref_rank)                                     AS mh_source
    FROM (
        SELECT store_id, product_variant_id, mh_id, mh_pref_rank, mh_source FROM cand_a
        UNION ALL
        SELECT store_id, product_variant_id, mh_id, mh_pref_rank, mh_source FROM cand_b
    ) c
    WHERE nullif(trim(mh_id), '') IS NOT NULL
    GROUP BY store_id, product_variant_id, mh_id
),

-- --------------------------------------------------------------------------------------
-- 6. REQUIREMENT  (PLACEHOLDER -- wire in your existing DS_requirement here).
--    Expected grain: one row per (store_id, product_variant_id) with the demand to fill.
--    Replace the FROM / column below with your real source; alias the demand as
--    `requirement` and the pipeline picks it up automatically.
-- --------------------------------------------------------------------------------------
ds_requirement AS (
    SELECT
        store_id,
        product_variant_id,
        requirement
    FROM gold.scratch.ds_requirement        -- <== TODO: replace with real DS_requirement source
),

-- --------------------------------------------------------------------------------------
-- 7. RCA BACKBONE: base LEFT JOIN everything. No rows dropped.
--    Grain: (store_id, product_variant_id, mh_id, expiry_bucket).
-- --------------------------------------------------------------------------------------
all_edges AS (
    SELECT
        b.store_id,
        b.product_variant_id,

        -- eligibility (PO validation)
        ps.po_row_cnt,
        coalesce(ps.has_open,   0)                              AS has_open_po,
        coalesce(ps.has_closed, 0)                              AS has_closed_po,
        CASE WHEN coalesce(ps.has_open, 0) = 1 THEN 'OPEN'
             WHEN coalesce(ps.has_closed, 0) = 1 THEN 'CLOSED'
             WHEN ps.po_row_cnt IS NULL THEN 'NO_PO_ROW'
             ELSE 'OTHER' END                                   AS base_validation_status,
        CASE WHEN coalesce(ps.has_open, 0) = 1 THEN 0 ELSE 1 END AS eligible,

        -- candidate MH
        cm.mh_id,
        cm.mh_pref_rank,
        cm.mh_source,

        -- MH type (dead / buying)
        CASE WHEN cm.mh_id IS NULL THEN NULL
             WHEN bs.mh_id IS NOT NULL THEN 'buying'
             ELSE 'dead' END                                    AS mh_type,

        -- inventory bucket
        iv.expiry_bucket,
        iv.mh_inventory_qty,

        -- requirement
        r.requirement
    FROM to_base b
    LEFT JOIN po_status     ps ON b.store_id = ps.store_id AND b.product_variant_id = ps.product_variant_id
    LEFT JOIN candidate_mh  cm ON b.store_id = cm.store_id AND b.product_variant_id = cm.product_variant_id
    LEFT JOIN inv_by_bucket iv ON cm.mh_id  = iv.mh_id    AND cm.product_variant_id = iv.product_variant_id
    LEFT JOIN buying_set    bs ON cm.mh_id  = bs.mh_id    AND cm.product_variant_id = bs.product_variant_id
    LEFT JOIN ds_requirement r ON b.store_id = r.store_id AND b.product_variant_id = r.product_variant_id
),

-- --------------------------------------------------------------------------------------
-- 8. ALLOCATABLE EDGES: only eligible rows with real inventory and real requirement.
--    Assign the 4-level priority tier.
-- --------------------------------------------------------------------------------------
alloc_input AS (
    SELECT
        *,
        CASE
            WHEN expiry_bucket = 'in_30'   AND mh_type = 'dead'   THEN 1
            WHEN expiry_bucket = 'in_30'   AND mh_type = 'buying' THEN 2
            WHEN expiry_bucket = 'post_30' AND mh_type = 'dead'   THEN 3
            WHEN expiry_bucket = 'post_30' AND mh_type = 'buying' THEN 4
        END AS priority_tier
    FROM all_edges
    WHERE eligible = 1
      AND mh_inventory_qty  > 0
      AND coalesce(requirement, 0) > 0
),

-- --------------------------------------------------------------------------------------
-- 9. PASS 1 - store waterfall: spread the store's requirement down its MH priority list.
--    Tie-break: highest inventory in the bucket first.
-- --------------------------------------------------------------------------------------
desired AS (
    SELECT
        *,
        sum(mh_inventory_qty) OVER (
            PARTITION BY store_id, product_variant_id
            ORDER BY priority_tier ASC, mh_inventory_qty DESC, mh_id, expiry_bucket
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS store_supply_cumsum
    FROM alloc_input
),

desired_calc AS (
    SELECT
        *,
        (store_supply_cumsum - mh_inventory_qty)                              AS store_supply_before,
        greatest(0,
            least(mh_inventory_qty,
                  requirement - (store_supply_cumsum - mh_inventory_qty)))     AS desired_alloc
    FROM desired
),

-- --------------------------------------------------------------------------------------
-- 10. PASS 2 - MH supply cap: split each shared (mh, sku, bucket) pool across the
--     competing stores, in priority order, capped by the pool size.
-- --------------------------------------------------------------------------------------
mh_capped AS (
    SELECT
        *,
        sum(desired_alloc) OVER (
            PARTITION BY mh_id, product_variant_id, expiry_bucket
            ORDER BY priority_tier ASC, requirement DESC, store_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS mh_demand_cumsum
    FROM desired_calc
),

final_alloc AS (
    SELECT
        store_id,
        product_variant_id,
        mh_id,
        expiry_bucket,
        priority_tier,
        store_supply_before,
        store_supply_cumsum,
        desired_alloc,
        (mh_demand_cumsum - desired_alloc)                                   AS mh_demand_before,
        greatest(0, mh_inventory_qty - (mh_demand_cumsum - desired_alloc))   AS mh_remaining_before,
        least(desired_alloc,
              greatest(0, mh_inventory_qty - (mh_demand_cumsum - desired_alloc))) AS final_alloc
    FROM mh_capped
)

-- --------------------------------------------------------------------------------------
-- 11. FINAL RCA: backbone LEFT JOIN allocation results. Reason explains every row.
-- --------------------------------------------------------------------------------------
SELECT
    e.store_id,
    e.product_variant_id,

    -- eligibility trace
    e.base_validation_status,
    e.eligible,
    e.po_row_cnt,
    e.has_open_po,
    e.has_closed_po,

    -- candidate MH trace
    e.mh_id,
    e.mh_source,
    e.mh_pref_rank,
    e.mh_type,

    -- inventory / requirement trace
    e.expiry_bucket,
    e.mh_inventory_qty,
    e.requirement,
    fa.priority_tier,

    -- pass 1 (store waterfall) trace
    fa.store_supply_before,
    fa.store_supply_cumsum,
    coalesce(fa.desired_alloc, 0)        AS desired_alloc,

    -- pass 2 (MH supply cap) trace
    fa.mh_demand_before,
    fa.mh_remaining_before,
    coalesce(fa.final_alloc, 0)          AS final_alloc,

    -- store-level roll-up
    sum(coalesce(fa.final_alloc, 0)) OVER (PARTITION BY e.store_id, e.product_variant_id) AS store_total_allocated,
    greatest(0, coalesce(e.requirement, 0)
                - sum(coalesce(fa.final_alloc, 0)) OVER (PARTITION BY e.store_id, e.product_variant_id)) AS store_unmet,

    -- human-readable RCA
    CASE
        WHEN e.base_validation_status = 'OPEN'             THEN 'SKIP_PO_OPEN'
        WHEN e.mh_id IS NULL                               THEN 'NO_CANDIDATE_MH'
        WHEN e.mh_inventory_qty IS NULL
          OR e.mh_inventory_qty = 0                        THEN 'NO_INVENTORY_IN_MH'
        WHEN coalesce(e.requirement, 0) <= 0              THEN 'NO_REQUIREMENT'
        WHEN coalesce(fa.final_alloc, 0) > 0
         AND fa.final_alloc >= fa.desired_alloc            THEN 'ALLOCATED'
        WHEN coalesce(fa.final_alloc, 0) > 0
         AND fa.final_alloc <  fa.desired_alloc            THEN 'ALLOCATED_SUPPLY_CAPPED'
        WHEN coalesce(fa.desired_alloc, 0) = 0            THEN 'REQ_MET_BY_HIGHER_PRIORITY_MH'
        WHEN coalesce(fa.final_alloc, 0) = 0
         AND coalesce(fa.desired_alloc, 0) > 0            THEN 'SUPPLY_EXHAUSTED_BY_OTHER_STORES'
        ELSE 'NOT_ALLOCATED'
    END AS reason,

    current_date() AS run_date
FROM all_edges e
LEFT JOIN final_alloc fa
    ON  e.store_id           = fa.store_id
    AND e.product_variant_id = fa.product_variant_id
    AND e.mh_id              <=> fa.mh_id
    AND e.expiry_bucket      <=> fa.expiry_bucket
;
