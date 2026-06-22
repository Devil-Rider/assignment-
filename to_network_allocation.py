"""
=======================================================================================
TO Network Allocation -- distributed tier-by-tier greedy (PySpark)
=======================================================================================
Reads ONE pre-built candidate "network" table (built in SQL, shared by the caller) and
runs the greedy MH allocation as an iterative loop, entirely in Spark so it scales to
~20M+ lines.

INPUT  (key = store_id x sku x mh_id x inv_bucket), columns:
    store_id, sku, mh_id, inv_bucket,
    dead_mh                  (1 = dead MH, 0 = buying MH),
    frequency                (1 = MH planned for the store tomorrow, 0 = not),
    inv                      (inventory of mh x sku in this bucket; repeats across stores),
    po_base_validation_status('closed' / 'open'),
    ds_requirement           (store x sku demand; repeats across the store's mh lines),
    final_ros                (carried for RCA).

GREEDY RULES (confirmed):
  * Hard filter: only frequency = 1 lines are eligible; po 'open' lines are skipped.
  * Tier order (outer loop):
        tier 1 : dead_mh = 1 & inv_bucket = expiry_in_30days
        tier 2 : dead_mh = 0 & inv_bucket = expiry_in_30days
        tier 3 : dead_mh = 1 & inv_bucket = expiry_post_30days
        tier 4 : dead_mh = 0 & inv_bucket = expiry_post_30days
  * Inner rounds within a tier:
      - each open store x sku picks its HIGHEST-inventory candidate MH (one per store),
      - within each MH the competing stores are stacked by ds_requirement DESC and the
        cumulative requirement is gated against the MH inventory:
            cum_req <= mh_inv  -> allocate the store's ENTIRE requirement, else 0
        (all-or-nothing; no partials),
      - inventory pool is reduced, fully-allocated stores are closed (final_mh_id set),
      - every tested line is marked "used" so unserved stores fall through to their next
        MH next round; repeat until the tier has no working set, then go to the next tier.

OUTPUTS:
  * line-level   (store x sku x mh x inv_bucket) : allocation_round, allocated_qty,
                   total_allocated (mh x sku x bucket), inv_remaining, eligible, ...
  * store x sku final : final_mh_id, allocated_qty, balance_requirement, reason, ...
=======================================================================================
"""

from datetime import date

from pyspark.sql import functions as F
from pyspark.sql import Window

# ---- configuration -------------------------------------------------------------------
INPUT_TABLE = "gold.scratch.to_network_base"              # <== user-built network table
OUT_LINE_TABLE = "gold.scratch.to_network_base_allocated"  # line-level result
OUT_FINAL_TABLE = "gold.scratch.to_network_allocation"     # store x sku final result

BUCKET_IN30 = "expiry_in_30days"
BUCKET_POST30 = "expiry_post_30days"

# (tier_no, dead_mh value, inv_bucket)  -- processed in this order.
TIERS = [
    (1, 1, BUCKET_IN30),
    (2, 0, BUCKET_IN30),
    (3, 1, BUCKET_POST30),
    (4, 0, BUCKET_POST30),
]

MAX_ROUNDS = 200      # global safety guard against runaway loops


# ---------------------------------------------------------------------------------------
def _standardize(df):
    """Select + cast the contract columns, defensively."""
    return df.select(
        F.col("store_id").cast("string").alias("store_id"),
        F.col("sku").cast("string").alias("sku"),
        F.col("mh_id").cast("string").alias("mh_id"),
        F.col("inv_bucket").cast("string").alias("inv_bucket"),
        F.col("dead_mh").cast("int").alias("dead_mh"),
        F.col("frequency").cast("int").alias("frequency"),
        F.col("inv").cast("double").alias("inv"),
        F.col("po_base_validation_status").cast("string").alias("po_base_validation_status"),
        F.col("ds_requirement").cast("double").alias("ds_requirement"),
        F.col("final_ros").cast("double").alias("final_ros"),
    )


def allocate(lines):
    """
    Core greedy. `lines` is the standardized network DataFrame.
    Returns (line_out, final_out) DataFrames. No table reads/writes here (testable).
    """
    lines = lines.localCheckpoint()

    po_ok = (F.col("po_base_validation_status").isNull()
             | (F.lower("po_base_validation_status") != F.lit("open")))

    # Eligible lines that can actually receive allocation.
    eligible_lines = lines.filter((F.col("frequency") == 1) & po_ok).localCheckpoint()

    # ---- initial state DataFrames ----------------------------------------------------
    # pool: shared inventory per (mh, sku, bucket)
    pool_state = (lines.groupBy("mh_id", "sku", "inv_bucket")
                  .agg(F.max("inv").alias("inv_balance"))
                  .localCheckpoint())

    # store: remaining requirement + outcome per (store, sku)
    store_state = (lines.groupBy("store_id", "sku")
                   .agg(F.coalesce(F.max("ds_requirement"), F.lit(0.0)).alias("balance_req"))
                   .withColumn("done", F.lit(0))
                   .withColumn("final_mh_id", F.lit(None).cast("string"))
                   .withColumn("final_bucket", F.lit(None).cast("string"))
                   .withColumn("final_tier", F.lit(None).cast("int"))
                   .withColumn("final_round", F.lit(None).cast("int"))
                   .withColumn("allocated_qty", F.lit(0.0))
                   .localCheckpoint())

    # used lines accumulator (starts empty)
    used_lines = (eligible_lines.select("store_id", "sku", "mh_id", "inv_bucket")
                  .limit(0)
                  .withColumn("allocation_round", F.lit(0).cast("int"))
                  .localCheckpoint())

    pick_w = Window.partitionBy("store_id", "sku").orderBy(F.col("inv_balance").desc(), F.col("mh_id"))
    cum_w = (Window.partitionBy("mh_id", "sku", "inv_bucket")
             .orderBy(F.col("balance_req").desc(), F.col("store_id"))
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))

    rnd = 0
    for tier_no, dead_val, bucket in TIERS:
        tier_lines = eligible_lines.filter(
            (F.col("dead_mh") == dead_val) & (F.col("inv_bucket") == bucket))

        while rnd < MAX_ROUNDS:
            # ---- working set: tier lines still open & unused, with live inventory -----
            work = (tier_lines
                    .join(pool_state.filter(F.col("inv_balance") > 0),
                          ["mh_id", "sku", "inv_bucket"])
                    .join(store_state.filter((F.col("done") == 0) & (F.col("balance_req") > 0))
                                     .select("store_id", "sku", "balance_req"),
                          ["store_id", "sku"])
                    .join(used_lines.select("store_id", "sku", "mh_id", "inv_bucket"),
                          ["store_id", "sku", "mh_id", "inv_bucket"], "left_anti"))

            # one MH per store x sku = the highest-inventory candidate
            cand = work.withColumn("rn", F.row_number().over(pick_w)).filter(F.col("rn") == 1).drop("rn")

            # cumsum gate (largest requirement first), all-or-nothing
            gated = (cand
                     .withColumn("cum_req", F.sum("balance_req").over(cum_w))
                     .withColumn("allocated_qty",
                                 F.when(F.col("cum_req") <= F.col("inv_balance"), F.col("balance_req"))
                                  .otherwise(F.lit(0.0)))
                     .select("store_id", "sku", "mh_id", "inv_bucket", "balance_req",
                             "inv_balance", "allocated_qty")
                     .localCheckpoint())

            if gated.count() == 0:
                break                                   # tier exhausted -> next tier

            rnd += 1

            # ---- update pool: subtract what was allocated from each (mh, sku, bucket) -
            pool_delta = gated.groupBy("mh_id", "sku", "inv_bucket").agg(
                F.sum("allocated_qty").alias("alloc_sum"))
            pool_state = (pool_state.join(pool_delta, ["mh_id", "sku", "inv_bucket"], "left")
                          .withColumn("inv_balance",
                                      F.col("inv_balance") - F.coalesce("alloc_sum", F.lit(0.0)))
                          .drop("alloc_sum")
                          .localCheckpoint())

            # ---- update store: close fully-allocated stores --------------------------
            winners = (gated.filter(F.col("allocated_qty") > 0)
                       .select("store_id", "sku",
                               F.col("mh_id").alias("w_mh"),
                               F.col("inv_bucket").alias("w_bucket"),
                               F.col("allocated_qty").alias("w_alloc")))
            store_state = (store_state.join(winners, ["store_id", "sku"], "left")
                           .withColumn("done", F.when(F.col("w_mh").isNotNull(), F.lit(1)).otherwise(F.col("done")))
                           .withColumn("final_mh_id", F.coalesce("final_mh_id", "w_mh"))
                           .withColumn("final_bucket", F.coalesce("final_bucket", "w_bucket"))
                           .withColumn("final_tier",
                                       F.when(F.col("w_mh").isNotNull() & F.col("final_tier").isNull(),
                                              F.lit(tier_no)).otherwise(F.col("final_tier")))
                           .withColumn("final_round",
                                       F.when(F.col("w_mh").isNotNull() & F.col("final_round").isNull(),
                                              F.lit(rnd)).otherwise(F.col("final_round")))
                           .withColumn("allocated_qty",
                                       F.when(F.col("w_mh").isNotNull(), F.col("w_alloc"))
                                        .otherwise(F.col("allocated_qty")))
                           .withColumn("balance_req",
                                       F.when(F.col("w_mh").isNotNull(), F.lit(0.0))
                                        .otherwise(F.col("balance_req")))
                           .drop("w_mh", "w_bucket", "w_alloc")
                           .localCheckpoint())

            # ---- mark every tested line as used (allocated or not) -------------------
            used_new = (gated.select("store_id", "sku", "mh_id", "inv_bucket")
                        .withColumn("allocation_round", F.lit(rnd)))
            used_lines = used_lines.unionByName(used_new).localCheckpoint()

    # ===================================================================================
    # Assemble outputs
    # ===================================================================================
    eligible_flag = ((F.col("frequency") == 1) & po_ok).cast("int")

    winners_final = store_state.filter(F.col("final_mh_id").isNotNull()).select(
        "store_id", "sku",
        F.col("final_mh_id"), F.col("final_bucket"), F.col("allocated_qty").alias("line_alloc"))

    total_by_pool = (winners_final.groupBy("final_mh_id", "sku", "final_bucket")
                     .agg(F.sum("line_alloc").alias("total_allocated")))

    # ---- line-level output (all input lines kept) ------------------------------------
    line_out = (lines
                .withColumn("eligible", eligible_flag)
                .join(used_lines, ["store_id", "sku", "mh_id", "inv_bucket"], "left")
                .join(winners_final.select(
                        F.col("store_id"), F.col("sku"),
                        F.col("final_mh_id").alias("mh_id"),
                        F.col("final_bucket").alias("inv_bucket"),
                        F.col("line_alloc").alias("allocated_qty")),
                      ["store_id", "sku", "mh_id", "inv_bucket"], "left")
                .join(total_by_pool.select(
                        F.col("final_mh_id").alias("mh_id"), F.col("sku"),
                        F.col("final_bucket").alias("inv_bucket"), "total_allocated"),
                      ["mh_id", "sku", "inv_bucket"], "left")
                .join(pool_state.select("mh_id", "sku", "inv_bucket",
                                        F.col("inv_balance").alias("inv_remaining")),
                      ["mh_id", "sku", "inv_bucket"], "left")
                .withColumn("allocated_qty", F.coalesce("allocated_qty", F.lit(0.0)))
                .withColumn("total_allocated", F.coalesce("total_allocated", F.lit(0.0)))
                .withColumn("run_date", F.lit(date.today()))
                .select("store_id", "sku", "mh_id", "inv_bucket", "dead_mh", "frequency",
                        "po_base_validation_status", "ds_requirement", "final_ros", "inv",
                        "eligible", "allocation_round", "allocated_qty", "total_allocated",
                        "inv_remaining", "run_date"))

    # ---- store x sku helper info for reasons -----------------------------------------
    cand_info = (lines.filter((F.col("frequency") == 1) & po_ok & (F.col("inv") > 0))
                 .groupBy("store_id", "sku")
                 .agg(F.countDistinct("mh_id").alias("n_candidate_mhs")))
    open_info = (lines.groupBy("store_id", "sku")
                 .agg(F.max(F.when(F.lower("po_base_validation_status") == "open", 1).otherwise(0))
                       .alias("has_open")))

    # ---- store x sku final output ----------------------------------------------------
    final_out = (store_state
                 .join(lines.groupBy("store_id", "sku")
                            .agg(F.coalesce(F.max("ds_requirement"), F.lit(0.0)).alias("ds_requirement")),
                       ["store_id", "sku"], "left")
                 .join(cand_info, ["store_id", "sku"], "left")
                 .join(open_info, ["store_id", "sku"], "left")
                 .withColumn("n_candidate_mhs", F.coalesce("n_candidate_mhs", F.lit(0)))
                 .withColumn("balance_requirement",
                             F.when(F.col("final_mh_id").isNotNull(), F.lit(0.0))
                              .otherwise(F.coalesce("ds_requirement", F.lit(0.0))))
                 .withColumn("reason",
                             F.when(F.coalesce("ds_requirement", F.lit(0.0)) <= 0, "NO_REQUIREMENT")
                              .when(F.col("final_mh_id").isNotNull(), "ALLOCATED")
                              .when(F.col("n_candidate_mhs") > 0, "UNALLOCATED")
                              .when(F.col("has_open") == 1, "SKIP_PO_OPEN")
                              .otherwise("NO_CANDIDATE"))
                 .withColumn("run_date", F.lit(date.today()))
                 .select("store_id", "sku", "ds_requirement", "n_candidate_mhs",
                         "final_mh_id", "final_bucket", "final_tier", "final_round",
                         "allocated_qty", "balance_requirement", "reason", "run_date"))

    return line_out, final_out


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    lines = _standardize(spark.table(INPUT_TABLE))
    line_out, final_out = allocate(lines)

    (line_out.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(OUT_LINE_TABLE))
    (final_out.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(OUT_FINAL_TABLE))

    print(f"Wrote line-level -> {OUT_LINE_TABLE}, store x sku final -> {OUT_FINAL_TABLE}")


if __name__ == "__main__":
    main()
