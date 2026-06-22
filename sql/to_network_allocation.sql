-- =====================================================================================
-- TO Network Allocation  (greedy, SQL-first) -- ONE MH per store x sku
-- =====================================================================================
-- Grain / primary key of the OUTPUT table : (store_id, product_variant_id)
--   -> exactly one row per store x sku, and exactly one chosen MH (final_mh_id).
--
-- Every column that fed the decision is carried into that single row, so the table is
-- self-contained for RCA (no need to join back to intermediates):
--   eligibility, requirement, chosen MH + its source/rank/type/bucket/priority,
--   the candidate set that was considered, the inventory-contention balances, the
--   allocated vs unmet quantity, and a human-readable `reason`.
--
-- MH selection priority (highest first):
--      1) expiry_in_30days  + dead   MH
--      2) expiry_in_30days  + buying MH
--      3) expiry_post_30days + dead   MH
--      4) expiry_post_30days + buying MH
--   Tie-break within a tier: MH holding the most inventory in that bucket wins.
--
-- Because only ONE MH is allowed per store x sku, there is no spillover to a second MH.
-- Inventory is still shared across stores, so the chosen MH's pool is split across the
-- stores that picked it (cumsum of requirement, biggest requirement first). A store whose
-- chosen MH is drained by others is reported as SUPPLY_EXHAUSTED (it does NOT fall back to
-- another MH -- that is the single-MH constraint). Move to Python later if fallback is wanted.
-- =====================================================================================

CREATE OR REPLACE TABLE gold.scratch.to_network_allocation AS

WITH
-- --------------------------------------------------------------------------------------
-- 1. INVENTORY: warehouse stock split into two expiry buckets.
--    expiry_raw_data_summary.store_id IS the warehouse / MH.
--    Split rule: (picking_date - today) <= 30 -> in_30 , else post_30.
-- --------------------------------------------------------------------------------------
inv_by_bucket AS (
    SELECT
        store_id                                                   AS mh_id,
        sku                                                        AS product_variant_id,
        CASE WHEN datediff(picking_date, current_date()) <= 30
             THEN 'in_30' ELSE 'post_30' END                       AS expiry_bucket,
        sum(available_qty)                                         AS mh_inventory_qty
    FROM gold.ops.expiry_raw_data_summary
    WHERE lower(inv_bucket) IN ('good', 'deep')
      AND upper(store_type) = 'WAREHOUSE'
      AND available_qty IS NOT NULL
    GROUP BY store_id, sku,
             CASE WHEN datediff(picking_date, current_date()) <= 30 THEN 'in_30' ELSE 'post_30' END
    HAVING sum(available_qty) > 0
),

-- --------------------------------------------------------------------------------------
-- 2. BASE: the universe we must build network for.
-- --------------------------------------------------------------------------------------
to_base AS (
    SELECT DISTINCT store_id, product_variant_id
    FROM gold.planning.to_base_table
),

-- --------------------------------------------------------------------------------------
-- 3. PO VALIDATION (eligibility): open PO -> skip; closed or no/NULL status -> build.
--    Kept as a flag (not a filter) so 'open' rows still appear in the final table.
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
-- 4. BUYING MH SET: (mh, sku) actively bought = closed PO on a live store.
--    Candidate (mh, sku) in this set -> "buying", else -> "dead".
-- --------------------------------------------------------------------------------------
buying_set AS (
    SELECT DISTINCT mh_id, product_variant_id
    FROM gold.planning.po_base_table
    WHERE lower(validation_status) = 'closed'
      AND lower(store_phase)       = 'live'
      AND mh_id IS NOT NULL
),

-- --------------------------------------------------------------------------------------
-- 5. CANDIDATE MHs.
--   5a primary  : final_sec_mh_selection (store x sku) -- unpivot 4 preference slots.
--   5b fallback : store_master_live (all MHs of the store) -- only when 5a is empty.
-- --------------------------------------------------------------------------------------
cand_a AS (
    SELECT store_id, product_variant_id, mh_pref_rank, mh_id, 'final_sec_mh_selection' AS mh_source
    FROM (
        SELECT store_id, product_variant_id,
               stack(4, 1, to_primary_mh, 2, to_secondary_mh,
                        3, to_tertiary_mh, 4, to_quaternary_mh) AS (mh_pref_rank, mh_id)
        FROM gold.scratch.final_sec_mh_selection
    ) s
    WHERE nullif(trim(mh_id), '') IS NOT NULL
),

cand_a_keys AS (
    SELECT DISTINCT store_id, product_variant_id FROM cand_a
),

store_master_mh AS (
    SELECT DISTINCT store_id, mh_id
    FROM (
        SELECT store_id,
               stack(4, to_primary_mh, to_secondary_mh,
                        to_tertiary_mh, to_quaternary_mh) AS (mh_id)
        FROM gold.ops.store_master_live
    ) u
    WHERE nullif(trim(mh_id), '') IS NOT NULL
),

cand_b AS (
    SELECT
        b.store_id,
        b.product_variant_id,
        CAST(99 AS INT)     AS mh_pref_rank,          -- fallback = lowest preference
        m.mh_id,
        'store_master_live' AS mh_source
    FROM to_base b
    LEFT ANTI JOIN cand_a_keys k
        ON b.store_id = k.store_id AND b.product_variant_id = k.product_variant_id
    JOIN store_master_mh m ON b.store_id = m.store_id
),

candidate_mh AS (
    SELECT
        store_id,
        product_variant_id,
        mh_id,
        min(mh_pref_rank)               AS mh_pref_rank,
        min_by(mh_source, mh_pref_rank) AS mh_source
    FROM (
        SELECT store_id, product_variant_id, mh_id, mh_pref_rank, mh_source FROM cand_a
        UNION ALL
        SELECT store_id, product_variant_id, mh_id, mh_pref_rank, mh_source FROM cand_b
    ) c
    WHERE nullif(trim(mh_id), '') IS NOT NULL
    GROUP BY store_id, product_variant_id, mh_id
),

-- --------------------------------------------------------------------------------------
-- 6. REQUIREMENT  (PLACEHOLDER -- wire in your real DS_requirement here).
--    Grain: one row per (store_id, product_variant_id), demand aliased as `requirement`.
-- --------------------------------------------------------------------------------------
ds_requirement AS (
    SELECT store_id, product_variant_id, requirement
    FROM gold.scratch.ds_requirement       -- <== TODO: replace with real DS_requirement source
),

-- --------------------------------------------------------------------------------------
-- 7. CANDIDATE EDGES: eligible store x sku  x  candidate MH (with inventory)  x  bucket.
--    Only rows that can actually supply (inventory > 0, requirement > 0) and are eligible.
--    priority_tier 1..4 stamped here.
-- --------------------------------------------------------------------------------------
cand_edges AS (
    SELECT
        b.store_id,
        b.product_variant_id,
        cm.mh_id,
        cm.mh_source,
        cm.mh_pref_rank,
        CASE WHEN bs.mh_id IS NOT NULL THEN 'buying' ELSE 'dead' END AS mh_type,
        iv.expiry_bucket,
        iv.mh_inventory_qty,
        r.requirement,
        CASE
            WHEN iv.expiry_bucket = 'in_30'   AND bs.mh_id IS NOT NULL THEN 2
            WHEN iv.expiry_bucket = 'in_30'   AND bs.mh_id IS NULL     THEN 1
            WHEN iv.expiry_bucket = 'post_30' AND bs.mh_id IS NOT NULL THEN 4
            WHEN iv.expiry_bucket = 'post_30' AND bs.mh_id IS NULL     THEN 3
        END AS priority_tier
    FROM to_base b
    JOIN po_status      ps ON b.store_id = ps.store_id AND b.product_variant_id = ps.product_variant_id
    JOIN candidate_mh   cm ON b.store_id = cm.store_id AND b.product_variant_id = cm.product_variant_id
    JOIN inv_by_bucket  iv ON cm.mh_id  = iv.mh_id     AND cm.product_variant_id = iv.product_variant_id
    JOIN ds_requirement r  ON b.store_id = r.store_id  AND b.product_variant_id = r.product_variant_id
    LEFT JOIN buying_set bs ON cm.mh_id = bs.mh_id     AND cm.product_variant_id = bs.product_variant_id
    WHERE coalesce(ps.has_open, 0) = 0          -- eligible only
      AND coalesce(r.requirement, 0) > 0        -- has a real requirement
),

-- --------------------------------------------------------------------------------------
-- 8. CANDIDATE SUMMARY per store x sku (for RCA): how many MHs were in play + a debug list.
-- --------------------------------------------------------------------------------------
cand_summary AS (
    SELECT
        store_id,
        product_variant_id,
        count(*)                                                              AS n_candidate_edges,
        count(DISTINCT mh_id)                                                 AS n_candidate_mhs,
        array_join(
            sort_array(collect_list(
                concat_ws(':', mh_id, expiry_bucket, mh_type,
                          cast(priority_tier AS string), cast(mh_inventory_qty AS string)))),
            ' | ')                                                            AS candidates_considered
    FROM cand_edges
    GROUP BY store_id, product_variant_id
),

-- --------------------------------------------------------------------------------------
-- 9. PICK ONE MH per store x sku: best priority tier, then most inventory.
-- --------------------------------------------------------------------------------------
chosen AS (
    SELECT *
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY store_id, product_variant_id
                ORDER BY priority_tier ASC, mh_inventory_qty DESC, mh_pref_rank ASC, mh_id
            ) AS rn
        FROM cand_edges
    ) z
    WHERE rn = 1
),

-- --------------------------------------------------------------------------------------
-- 10. SHARED-INVENTORY CAP on the chosen MH: split each (mh, sku, bucket) pool across the
--     stores that chose it (biggest requirement first), capped by the pool.
-- --------------------------------------------------------------------------------------
chosen_capped AS (
    SELECT
        *,
        sum(requirement) OVER (
            PARTITION BY mh_id, product_variant_id, expiry_bucket
            ORDER BY requirement DESC, store_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS mh_demand_cumsum
    FROM chosen
),

allocated AS (
    SELECT
        store_id,
        product_variant_id,
        mh_id              AS final_mh_id,
        mh_source,
        mh_pref_rank,
        mh_type,
        expiry_bucket,
        priority_tier,
        mh_inventory_qty,
        requirement,
        (mh_demand_cumsum - requirement)                                 AS mh_demand_before,
        greatest(0, mh_inventory_qty - (mh_demand_cumsum - requirement)) AS mh_remaining_before,
        least(requirement,
              greatest(0, mh_inventory_qty - (mh_demand_cumsum - requirement))) AS allocated_qty
    FROM chosen_capped
)

-- --------------------------------------------------------------------------------------
-- 11. FINAL TABLE: one row per store x sku (PK), every decision column attached.
--     Base LEFT JOIN everything so store x sku with no MH / open PO / no req still appear.
-- --------------------------------------------------------------------------------------
SELECT
    b.store_id,
    b.product_variant_id,

    -- requirement
    rq.requirement,

    -- eligibility trace
    coalesce(ps.po_row_cnt, 0)                                  AS po_row_cnt,
    coalesce(ps.has_open,   0)                                  AS has_open_po,
    coalesce(ps.has_closed, 0)                                  AS has_closed_po,
    CASE WHEN coalesce(ps.has_open, 0) = 1 THEN 'OPEN'
         WHEN coalesce(ps.has_closed, 0) = 1 THEN 'CLOSED'
         WHEN ps.po_row_cnt IS NULL THEN 'NO_PO_ROW'
         ELSE 'OTHER' END                                       AS base_validation_status,
    CASE WHEN coalesce(ps.has_open, 0) = 1 THEN 0 ELSE 1 END    AS eligible,

    -- candidate set considered (RCA)
    coalesce(cs.n_candidate_mhs, 0)                             AS n_candidate_mhs,
    coalesce(cs.n_candidate_edges, 0)                           AS n_candidate_edges,
    cs.candidates_considered,

    -- the chosen MH + the columns that picked it
    a.final_mh_id,
    a.mh_source,
    a.mh_pref_rank,
    a.mh_type,
    a.expiry_bucket,
    a.priority_tier,
    a.mh_inventory_qty,

    -- inventory-contention trace
    a.mh_demand_before,
    a.mh_remaining_before,
    coalesce(a.allocated_qty, 0)                                AS allocated_qty,
    greatest(0, coalesce(rq.requirement, 0) - coalesce(a.allocated_qty, 0)) AS unmet_qty,

    -- human-readable RCA
    CASE
        WHEN coalesce(ps.has_open, 0) = 1                 THEN 'SKIP_PO_OPEN'
        WHEN coalesce(rq.requirement, 0) <= 0            THEN 'NO_REQUIREMENT'
        WHEN cs.n_candidate_mhs IS NULL                   THEN 'NO_CANDIDATE_MH_WITH_INVENTORY'
        WHEN a.allocated_qty >= rq.requirement            THEN 'ALLOCATED'
        WHEN a.allocated_qty > 0                          THEN 'ALLOCATED_PARTIAL_SUPPLY_CAPPED'
        ELSE 'SUPPLY_EXHAUSTED_BY_OTHER_STORES'
    END                                                        AS reason,

    current_date()                                             AS run_date
FROM to_base b
LEFT JOIN po_status      ps ON b.store_id = ps.store_id AND b.product_variant_id = ps.product_variant_id
LEFT JOIN ds_requirement rq ON b.store_id = rq.store_id AND b.product_variant_id = rq.product_variant_id
LEFT JOIN cand_summary   cs ON b.store_id = cs.store_id AND b.product_variant_id = cs.product_variant_id
LEFT JOIN allocated      a  ON b.store_id = a.store_id  AND b.product_variant_id = a.product_variant_id
;
