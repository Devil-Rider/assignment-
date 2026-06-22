"""
Spark-local check for the distributed greedy -- no Databricks needed.

Builds a tiny in-memory network DataFrame, runs allocate(), and asserts the outcome.
Run:  python tests/sample_check.py   (requires pyspark installed locally)

Scenario (one sku 'K'), tier-1 = dead_mh & expiry_in_30days:
  MH 'X' (dead, in_30) inv=30 is shared by S1,S2,S3 (each req 20); MH 'Y' (buying, in_30)
  inv=100 is the fallback for S2/S3. S4's only line is frequency=0 (ineligible). S5 is PO open.
  S6 has a candidate but needs more than any MH holds (req 999) -> UNALLOCATED.

Expected (largest-req-first, all-or-nothing, fall-through):
  Round 1, tier-1: all three want X (=30). Stacked 20,40,60 -> only S1 fits (20) -> X.
  S2,S3 get 0, are marked used on X, and FALL THROUGH.
  Tier-2 (buying in_30): S2,S3 take Y (=100) -> Y.
  S4 -> NO_CANDIDATE (frequency=0 filtered out).
  S5 -> SKIP_PO_OPEN.
  S6 -> UNALLOCATED.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from pyspark.sql import SparkSession
except ImportError:
    print("SKIP: pyspark not installed")
    sys.exit(0)

from to_network_allocation import allocate, _standardize, BUCKET_IN30  # noqa: E402


def row(store, mh, dead, freq, inv, req, po="closed", bucket=BUCKET_IN30, ros=1.0):
    return (store, "K", mh, bucket, dead, freq, float(inv), po, float(req), ros)


def main():
    spark = (SparkSession.builder.master("local[2]").appName("alloc-test")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    cols = ["store_id", "sku", "mh_id", "inv_bucket", "dead_mh", "frequency",
            "inv", "po_base_validation_status", "ds_requirement", "final_ros"]
    data = [
        row("S1", "X", 1, 1, 30, 20),
        row("S2", "X", 1, 1, 30, 20), row("S2", "Y", 0, 1, 100, 20),
        row("S3", "X", 1, 1, 30, 20), row("S3", "Y", 0, 1, 100, 20),
        row("S4", "X", 1, 0, 30, 20),                       # frequency=0 -> ineligible
        row("S5", "X", 1, 1, 30, 20, po="open"),            # PO open -> skip
        row("S6", "Z", 1, 1, 5, 999),                       # needs more than any MH -> unallocated
    ]
    lines = _standardize(spark.createDataFrame(data, cols))

    _, final_out = allocate(lines)
    res = {r["store_id"]: r for r in final_out.collect()}

    def check(store, reason, mh=None):
        r = res[store]
        ok = r["reason"] == reason and (mh is None or r["final_mh_id"] == mh)
        print(f"  {store}: reason={r['reason']:<12} final_mh={r['final_mh_id']} "
              f"alloc={r['allocated_qty']} bal={r['balance_requirement']}  "
              f"{'OK' if ok else 'FAIL <<<'}")
        assert ok, f"{store}: expected {reason}/{mh}, got {r['reason']}/{r['final_mh_id']}"

    print("results:")
    check("S1", "ALLOCATED", "X")
    check("S2", "ALLOCATED", "Y")
    check("S3", "ALLOCATED", "Y")
    check("S4", "NO_CANDIDATE")
    check("S5", "SKIP_PO_OPEN")
    check("S6", "UNALLOCATED")
    print("ALL ASSERTIONS PASSED")

    spark.stop()


if __name__ == "__main__":
    main()
