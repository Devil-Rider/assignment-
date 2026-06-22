"""
Local check for the greedy allocation -- no Spark, no Databricks needed.

Builds dummy `edges_pdf` / `base_pdf` (the exact shapes that QUERY_EDGES / QUERY_BASE
return) for one sku, runs build_output(), and prints the store x sku RCA result.

Run:  python tests/sample_check.py

Scenario (SKU1), warehouses MH1 (dead), MH2 (buying), MH3 (dead, fallback):
  inventory  : MH1 in_30=30, MH1 post_30=50, MH2 in_30=100, MH3 in_30=5
  S1 req10   candidates MH1(1), MH2(2)
  S2 req10   candidates MH1(1), MH2(2)
  S3 req15   candidates MH1(1), MH2(2)
  S4 req8    candidate  MH3(99, fallback)
  S5 req5    PO 'open'  -> skipped (appears in base only)

Expected:
  S3 -> MH1 in_30 tier1 alloc15 (largest req grabs dead/in_30 first)
  S1 -> MH1 in_30 tier1 alloc10 (MH1 now drained to 5)
  S2 -> falls through (MH1 left=5 < 10) -> MH2 in_30 tier2 alloc10
  S4 -> no MH can fully serve -> best-partial MH3 in_30 alloc5, unmet3
  S5 -> SKIP_PO_OPEN, alloc0 unmet5
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from to_network_allocation import build_output  # noqa: E402


def edge(store, mh, rank, mh_type, bucket, inv, req, tier, source="final_sec_mh_selection"):
    return dict(store_id=store, product_variant_id="SKU1", mh_id=mh, mh_source=source,
                mh_pref_rank=rank, mh_type=mh_type, expiry_bucket=bucket,
                mh_inventory_qty=inv, requirement=req, priority_tier=tier)


def main():
    edges = pd.DataFrame([
        # S1, S2, S3 : MH1 (dead) in_30 & post_30, plus MH2 (buying) in_30
        edge("S1", "MH1", 1, "dead",   "in_30",   30, 10, 1),
        edge("S1", "MH1", 1, "dead",   "post_30", 50, 10, 3),
        edge("S1", "MH2", 2, "buying", "in_30",  100, 10, 2),
        edge("S2", "MH1", 1, "dead",   "in_30",   30, 10, 1),
        edge("S2", "MH1", 1, "dead",   "post_30", 50, 10, 3),
        edge("S2", "MH2", 2, "buying", "in_30",  100, 10, 2),
        edge("S3", "MH1", 1, "dead",   "in_30",   30, 15, 1),
        edge("S3", "MH1", 1, "dead",   "post_30", 50, 15, 3),
        edge("S3", "MH2", 2, "buying", "in_30",  100, 15, 2),
        # S4 : only the store_master_live fallback MH3 (dead) in_30 = 5
        edge("S4", "MH3", 99, "dead", "in_30", 5, 8, 1, source="store_master_live"),
    ])

    base = pd.DataFrame([
        dict(store_id="S1", product_variant_id="SKU1", requirement=10, po_row_cnt=1,
             has_open_po=0, has_closed_po=1, base_validation_status="CLOSED", eligible=1),
        dict(store_id="S2", product_variant_id="SKU1", requirement=10, po_row_cnt=1,
             has_open_po=0, has_closed_po=1, base_validation_status="CLOSED", eligible=1),
        dict(store_id="S3", product_variant_id="SKU1", requirement=15, po_row_cnt=1,
             has_open_po=0, has_closed_po=1, base_validation_status="CLOSED", eligible=1),
        dict(store_id="S4", product_variant_id="SKU1", requirement=8, po_row_cnt=0,
             has_open_po=0, has_closed_po=0, base_validation_status="NO_PO_ROW", eligible=1),
        dict(store_id="S5", product_variant_id="SKU1", requirement=5, po_row_cnt=1,
             has_open_po=1, has_closed_po=0, base_validation_status="OPEN", eligible=0),
    ])

    out = build_output(edges, base)
    show = ["store_id", "requirement", "base_validation_status", "final_mh_id",
            "final_mh_type", "final_expiry_bucket", "final_priority_tier",
            "allocated_qty", "unmet_qty", "reason"]
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", None)
    print(out[show].to_string(index=False))


if __name__ == "__main__":
    main()
