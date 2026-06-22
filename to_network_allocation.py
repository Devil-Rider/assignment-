"""
=======================================================================================
TO Network Allocation -- greedy, ONE MH per store x sku, with fallback   (PySpark + pandas)
=======================================================================================
Output table grain / primary key : (store_id, product_variant_id)
  -> exactly one row per store x sku, and exactly one chosen MH (final_mh_id).

Greedy rules (confirmed):
  * Priority order of supply (highest first):
        tier 1 : expiry_in_30days  + DEAD   MH
        tier 2 : expiry_in_30days  + BUYING MH
        tier 3 : expiry_post_30days + DEAD   MH
        tier 4 : expiry_post_30days + BUYING MH
    (a "dead" MH = an MH that is NOT actively bought for that sku;
     a "buying" MH = closed PO on a live store, from po_base_table.)
  * Processed GLOBALLY tier by tier: all tier-1 demand is served before any tier-2, etc.
  * One MH per store x sku WITH FALLBACK: a store takes ALL its qty from a single MH.
    If its preferred MH cannot fully serve it (drained by other stores), the store
    "falls through" to its next candidate MH (LH secondary/tertiary/quaternary) / next
    tier until one MH can fully serve it.  (e.g. one shared MH covers 30 stores' full
    requirement -> those 30 are done; the other 70 fall through to their next MH.)
  * Contention: when a shared MH cannot serve everyone, the stores with the LARGEST
    requirement win the inventory first.
  * Stores that no single MH can fully serve fall to a final "best partial" pass
    (assigned to the candidate MH with the most remaining inventory) and report unmet qty.

Every column used in the decision is written to the output row so the table is
self-contained for RCA.

NOTE on scope of "one MH": a store draws from a single (mh, expiry_bucket) slot.
Combining both buckets of the SAME MH to complete a requirement is a deliberate
future refinement (kept out for v1 clarity).

NOTE on performance: the greedy is O(n log n) per sku (single pass per tier) and the
problem decomposes by sku.  The candidate edges are pulled to the driver via pandas;
if the edge set is very large, batch the sku groups (see allocate_all).
=======================================================================================
"""

from datetime import date

import pandas as pd
from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------------------
# Shared CTEs -- build the inputs exactly once, reused by both the EDGES and BASE queries.
# Swap the ds_requirement CTE for your real DS_requirement source (alias demand as
# `requirement`, grain store_id x product_variant_id).
# ---------------------------------------------------------------------------------------
WITH_CLAUSE = """
WITH
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
to_base AS (
    SELECT DISTINCT store_id, product_variant_id
    FROM gold.planning.to_base_table
),
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
buying_set AS (
    SELECT DISTINCT mh_id, product_variant_id
    FROM gold.planning.po_base_table
    WHERE lower(validation_status) = 'closed'
      AND lower(store_phase)       = 'live'
      AND mh_id IS NOT NULL
),
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
    SELECT b.store_id, b.product_variant_id,
           CAST(99 AS INT) AS mh_pref_rank, m.mh_id, 'store_master_live' AS mh_source
    FROM to_base b
    LEFT ANTI JOIN cand_a_keys k
        ON b.store_id = k.store_id AND b.product_variant_id = k.product_variant_id
    JOIN store_master_mh m ON b.store_id = m.store_id
),
candidate_mh AS (
    SELECT store_id, product_variant_id, mh_id,
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
ds_requirement AS (
    SELECT store_id, product_variant_id, requirement
    FROM gold.scratch.ds_requirement   -- <== TODO: replace with real DS_requirement source
)
"""

# Candidate edges = eligible store x sku  x  candidate MH (that has inventory)  x  bucket.
QUERY_EDGES = WITH_CLAUSE + """
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
        WHEN iv.expiry_bucket = 'in_30'   AND bs.mh_id IS NULL     THEN 1
        WHEN iv.expiry_bucket = 'in_30'   AND bs.mh_id IS NOT NULL THEN 2
        WHEN iv.expiry_bucket = 'post_30' AND bs.mh_id IS NULL     THEN 3
        WHEN iv.expiry_bucket = 'post_30' AND bs.mh_id IS NOT NULL THEN 4
    END AS priority_tier
FROM to_base b
JOIN po_status      ps ON b.store_id = ps.store_id AND b.product_variant_id = ps.product_variant_id
JOIN candidate_mh   cm ON b.store_id = cm.store_id AND b.product_variant_id = cm.product_variant_id
JOIN inv_by_bucket  iv ON cm.mh_id  = iv.mh_id     AND cm.product_variant_id = iv.product_variant_id
JOIN ds_requirement r  ON b.store_id = r.store_id  AND b.product_variant_id = r.product_variant_id
LEFT JOIN buying_set bs ON cm.mh_id = bs.mh_id     AND cm.product_variant_id = bs.product_variant_id
WHERE coalesce(ps.has_open, 0) = 0
  AND coalesce(r.requirement, 0) > 0
"""

# Base = every store x sku (so ineligible / no-candidate / no-requirement rows still appear).
QUERY_BASE = WITH_CLAUSE + """
SELECT
    b.store_id,
    b.product_variant_id,
    rq.requirement,
    coalesce(ps.po_row_cnt, 0)                                  AS po_row_cnt,
    coalesce(ps.has_open,   0)                                  AS has_open_po,
    coalesce(ps.has_closed, 0)                                  AS has_closed_po,
    CASE WHEN coalesce(ps.has_open, 0) = 1 THEN 'OPEN'
         WHEN coalesce(ps.has_closed, 0) = 1 THEN 'CLOSED'
         WHEN ps.po_row_cnt IS NULL THEN 'NO_PO_ROW'
         ELSE 'OTHER' END                                       AS base_validation_status,
    CASE WHEN coalesce(ps.has_open, 0) = 1 THEN 0 ELSE 1 END    AS eligible
FROM to_base b
LEFT JOIN po_status      ps ON b.store_id = ps.store_id AND b.product_variant_id = ps.product_variant_id
LEFT JOIN ds_requirement rq ON b.store_id = rq.store_id AND b.product_variant_id = rq.product_variant_id
"""

OUTPUT_TABLE = "gold.scratch.to_network_allocation"
TIERS = (1, 2, 3, 4)


# ---------------------------------------------------------------------------------------
# Greedy allocation for a SINGLE sku.
# ---------------------------------------------------------------------------------------
def allocate_one_sku(sku, edges):
    """
    edges : DataFrame of candidate edges for one sku, columns:
        store_id, mh_id, mh_source, mh_pref_rank, mh_type, expiry_bucket,
        mh_inventory_qty, requirement, priority_tier
    Returns list of dict, one per store in this sku that had at least one candidate edge.
    """
    # Inventory pools per (mh, bucket) -- the shared, decremented resource.
    pools, pool_init = {}, {}
    for (mh, bkt), sub in edges.groupby(["mh_id", "expiry_bucket"]):
        qty = float(sub["mh_inventory_qty"].iloc[0])
        pools[(mh, bkt)] = qty
        pool_init[(mh, bkt)] = qty

    # Per-store: requirement + candidate slots grouped by tier.
    requirement = edges.groupby("store_id")["requirement"].first().astype(float).to_dict()
    remaining = dict(requirement)
    assigned = {}                       # store -> dict(mh, bucket, tier, alloc, lh_rank, mh_type, mh_source)

    slots_by_tier = {t: {} for t in TIERS}     # tier -> store -> list of slot dicts
    for row in edges.itertuples(index=False):
        slot = dict(mh=row.mh_id, bucket=row.expiry_bucket, lh_rank=int(row.mh_pref_rank),
                    mh_type=row.mh_type, mh_source=row.mh_source)
        slots_by_tier[int(row.priority_tier)].setdefault(row.store_id, []).append(slot)

    # ---- Pass A: tier by tier, FULL-service only, largest requirement first. -----------
    # Pools only shrink within a tier, so a single largest-first pass per tier is correct.
    for tier in TIERS:
        eligible = [s for s in slots_by_tier[tier]
                    if s not in assigned and remaining[s] > 0]
        eligible.sort(key=lambda s: (-remaining[s], s))          # largest requirement first
        for s in eligible:
            need = remaining[s]
            # candidate slots in this tier that can FULLY serve `need`, best by (lh_rank, pool desc)
            opts = [sl for sl in slots_by_tier[tier][s] if pools.get((sl["mh"], sl["bucket"]), 0) >= need]
            if not opts:
                continue                                          # fall through to next tier
            opts.sort(key=lambda sl: (sl["lh_rank"], -pools[(sl["mh"], sl["bucket"])]))
            sl = opts[0]
            pools[(sl["mh"], sl["bucket"])] -= need
            remaining[s] = 0.0
            assigned[s] = dict(sl, tier=tier, alloc=need)

    # ---- Pass B: best-partial cleanup for stores no single MH could fully serve. --------
    all_slots = {}                       # store -> list of (tier, slot) across every tier
    for tier in TIERS:
        for s, slist in slots_by_tier[tier].items():
            for sl in slist:
                all_slots.setdefault(s, []).append((tier, sl))

    leftover = [s for s in requirement if s not in assigned and remaining[s] > 0]
    leftover.sort(key=lambda s: (-remaining[s], s))               # largest requirement first
    for s in leftover:
        opts = [(t, sl) for (t, sl) in all_slots.get(s, [])
                if pools.get((sl["mh"], sl["bucket"]), 0) > 0]
        if not opts:
            continue                                              # nothing left anywhere -> exhausted
        opts.sort(key=lambda ts: (ts[0], ts[1]["lh_rank"], -pools[(ts[1]["mh"], ts[1]["bucket"])]))
        tier, sl = opts[0]
        take = min(remaining[s], pools[(sl["mh"], sl["bucket"])])
        pools[(sl["mh"], sl["bucket"])] -= take
        remaining[s] -= take
        assigned[s] = dict(sl, tier=tier, alloc=take)

    # ---- Emit one record per store that had candidate edges for this sku. --------------
    out = []
    for s, req in requirement.items():
        a = assigned.get(s)
        out.append(dict(
            store_id=s,
            product_variant_id=sku,
            final_mh_id=a["mh"] if a else None,
            final_mh_source=a["mh_source"] if a else None,
            final_mh_pref_rank=a["lh_rank"] if a else None,
            final_mh_type=a["mh_type"] if a else None,
            final_expiry_bucket=a["bucket"] if a else None,
            final_priority_tier=a["tier"] if a else None,
            final_mh_inventory_qty=pool_init[(a["mh"], a["bucket"])] if a else None,
            allocated_qty=float(a["alloc"]) if a else 0.0,
            unmet_qty=float(req - (a["alloc"] if a else 0.0)),
        ))
    return out


def allocate_all(edges_pdf):
    """Run the greedy for every sku and return a tidy pandas DataFrame (store x sku grain)."""
    records = []
    for sku, edges in edges_pdf.groupby("product_variant_id"):
        records.extend(allocate_one_sku(sku, edges))
    return pd.DataFrame.from_records(records)


# ---------------------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------------------
def main():
    spark = SparkSession.builder.getOrCreate()

    edges_pdf = spark.sql(QUERY_EDGES).toPandas()
    base_pdf = spark.sql(QUERY_BASE).toPandas()

    # Candidate-set summary per store x sku (RCA): how many MHs (with inventory) were in play.
    if len(edges_pdf):
        edges_pdf["_cand"] = (
            edges_pdf["mh_id"].astype(str) + ":" + edges_pdf["expiry_bucket"].astype(str)
            + ":" + edges_pdf["mh_type"].astype(str) + ":t" + edges_pdf["priority_tier"].astype(str)
            + ":" + edges_pdf["mh_inventory_qty"].astype(str)
        )
        cand_summary = (
            edges_pdf.groupby(["store_id", "product_variant_id"])
            .agg(n_candidate_mhs=("mh_id", "nunique"),
                 n_candidate_edges=("mh_id", "size"),
                 candidates_considered=("_cand", lambda x: " | ".join(sorted(x))))
            .reset_index()
        )
        alloc_pdf = allocate_all(edges_pdf.drop(columns="_cand"))
    else:
        cand_summary = pd.DataFrame(
            columns=["store_id", "product_variant_id", "n_candidate_mhs",
                     "n_candidate_edges", "candidates_considered"])
        alloc_pdf = pd.DataFrame()

    # Assemble the final store x sku table: base LEFT JOIN summary LEFT JOIN allocation.
    out = base_pdf.merge(cand_summary, on=["store_id", "product_variant_id"], how="left")
    if len(alloc_pdf):
        out = out.merge(alloc_pdf, on=["store_id", "product_variant_id"], how="left")
    else:
        for col in ["final_mh_id", "final_mh_source", "final_mh_pref_rank", "final_mh_type",
                    "final_expiry_bucket", "final_priority_tier", "final_mh_inventory_qty",
                    "allocated_qty", "unmet_qty"]:
            out[col] = None

    out["n_candidate_mhs"] = out["n_candidate_mhs"].fillna(0).astype(int)
    out["n_candidate_edges"] = out["n_candidate_edges"].fillna(0).astype(int)
    out["allocated_qty"] = out["allocated_qty"].fillna(0.0)
    out["unmet_qty"] = out["unmet_qty"].fillna(out["requirement"]).fillna(0.0)

    # Human-readable RCA reason.
    def reason(r):
        if r["has_open_po"] == 1:
            return "SKIP_PO_OPEN"
        if not r["requirement"] or r["requirement"] <= 0:
            return "NO_REQUIREMENT"
        if r["n_candidate_mhs"] == 0:
            return "NO_CANDIDATE_MH_WITH_INVENTORY"
        if pd.isna(r["final_mh_id"]):
            return "SUPPLY_EXHAUSTED_BY_OTHER_STORES"
        if r["unmet_qty"] <= 0:
            return "ALLOCATED"
        return "ALLOCATED_PARTIAL_SUPPLY_CAPPED"

    out["reason"] = out.apply(reason, axis=1)
    out["run_date"] = date.today()

    cols = ["store_id", "product_variant_id", "requirement",
            "base_validation_status", "eligible", "po_row_cnt", "has_open_po", "has_closed_po",
            "n_candidate_mhs", "n_candidate_edges", "candidates_considered",
            "final_mh_id", "final_mh_source", "final_mh_pref_rank", "final_mh_type",
            "final_expiry_bucket", "final_priority_tier", "final_mh_inventory_qty",
            "allocated_qty", "unmet_qty", "reason", "run_date"]
    out = out[cols]

    (spark.createDataFrame(out)
        .write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(OUTPUT_TABLE))

    print(f"Wrote {len(out)} store x sku rows to {OUTPUT_TABLE}")


if __name__ == "__main__":
    main()
