# Understanding `to_network_allocation.py` — From First Principles

This document explains, from the ground up, what the allocation script does, **why** it
does it that way, and **how** every piece of the code maps onto the business problem. It
assumes you know basic SQL/tables but does **not** assume you know PySpark. We build the
intuition first, then read the code.

---

## Part 0 — The business problem in plain words

We run dark stores (the small fulfilment stores that serve quick-commerce orders). Each
store needs to be re-stocked. The inventory it needs can be supplied from a **mother hub
(MH)** — a larger warehouse. A **Transfer Order (TO)** is the instruction "move X units of
SKU Y from mother-hub Z to store S".

The job of this script is to **decide, for every (store, SKU) that needs stock tomorrow,
which mother hub should supply it, and how much** — given that:

- Mother-hub inventory is **limited and shared**. If two stores both want the same SKU
  from the same MH, they compete for the same physical units.
- We **prefer to drain certain inventory first** — specifically inventory that is about
  to expire, and inventory sitting in "dead" mother hubs we want to wind down. Better to
  ship goods that would otherwise be thrown away than to ship fresh long-dated stock.
- Each store × SKU should be served by **exactly one** mother hub (operationally you don't
  want a single store receiving the same SKU split across five hubs — that's five trucks,
  five receiving events). So the rule is **one MH per store × SKU**, and it's
  **all-or-nothing**: a hub either covers the store's *entire* requirement or it gives it
  nothing and the store looks elsewhere.

So this is a **constrained assignment / allocation** problem. There is no clean closed-form
SQL answer because the decisions are *sequential and competitive*: who I give inventory to
first changes what's left for everyone else. That sequential, "take the best still
available, subtract it, repeat" shape is what we call a **greedy algorithm**.

---

## Part 1 — The core idea: a greedy algorithm

A greedy algorithm solves a big optimisation by repeatedly making the **locally best
choice** and never undoing it. It doesn't guarantee a mathematically perfect global
optimum, but it's fast, explainable, and matches how a human planner would actually do
this with a spreadsheet:

> "Take the most urgent inventory. Hand it to the stores that want it, biggest first,
> until it runs out. Cross those stores off. Look at what's left. Repeat."

Two ideas make a greedy algorithm work:

1. **A priority order** — what do we serve first? Here it's encoded as **tiers** (urgent
   inventory before non-urgent) and, inside a tier, **biggest requirement first**.
2. **State that shrinks** — every time we allocate, we **subtract** from the inventory
   pool and **remove** the satisfied stores from the working set. The problem gets
   strictly smaller every round, which is why the loop terminates.

Everything in this file is an implementation of those two ideas at scale.

---

## Part 2 — Why PySpark (and what Spark actually is)

We have ~20M+ candidate lines (every store × SKU × candidate-MH × inventory-bucket
combination). That's too big to pull into a single machine's memory and loop over row by
row in plain Python — it would be slow and would crash.

**Spark** is a distributed computation engine. You describe your data as **DataFrames**
(think: big distributed tables) and you describe *transformations* on them (filter, join,
group, etc.). Spark splits the data across many machines ("executors") and runs the work in
parallel. Key mental model:

- A Spark DataFrame is **lazy**. When you write `df.filter(...)`, nothing computes yet. You
  are building a *recipe* (a query plan). It only actually runs when you call an **action**
  like `.count()`, `.write...`, or `.localCheckpoint()`.
- This laziness is powerful but dangerous in a loop: if every round just appends to the
  recipe without ever "baking" it, the recipe grows enormous and Spark eventually chokes
  trying to re-plan the whole history. That's why this code uses **`localCheckpoint()`** —
  it forces Spark to actually compute the current DataFrame, save the result, and **cut the
  lineage** (forget how it was made). Each loop iteration therefore starts from a concrete,
  finite table rather than an ever-growing recipe. This is *the* key trick that makes an
  iterative greedy algorithm survive in Spark.

So the whole strategy is: **express the greedy round as a set of DataFrame operations, and
run that round repeatedly, checkpointing the state between rounds.**

---

## Part 3 — The input contract

The script does **not** build the candidate set itself. Someone (a SQL step upstream)
builds one table — `gold.scratch.to_network_base` — where each row is one **candidate
edge**: "this store × SKU *could* be served by this MH from this inventory bucket."

The grain (unique key) of a row is:

```
store_id  ×  sku  ×  mh_id  ×  inv_bucket
```

The columns and what they mean:

| Column | Meaning |
|---|---|
| `store_id` | The store that needs stock. |
| `sku` | The product. |
| `mh_id` | A candidate mother hub that could supply it. |
| `inv_bucket` | Which inventory bucket of that hub — `expiry_in_30days` (about to expire) or `expiry_post_30days` (long-dated). |
| `dead_mh` | `1` = this is a "dead" hub we want to drain; `0` = a normal "buying" hub. |
| `frequency` | `1` = this MH is actually scheduled to ship to this store tomorrow; `0` = not. Only `1` is usable. |
| `inv` | Inventory available for this `mh × sku × bucket`. Note: this value **repeats** across every store row that points at the same hub/sku/bucket, because it's a *shared* pool, not a per-store number. |
| `po_base_validation_status` | `'open'` or `'closed'`. `'open'` means there's an unresolved purchase order — we must **skip** these lines. |
| `ds_requirement` | How much this store × SKU needs. Repeats across that store's candidate-MH rows (it's a property of the store×SKU, not of the edge). |
| `final_ros` | Rate-of-sale, carried through untouched purely for later root-cause analysis. |

The two important "repeats across rows" facts are subtle and matter a lot:

- `inv` is a **shared resource** keyed by `(mh, sku, bucket)` — many candidate rows point
  at the *same* physical inventory.
- `ds_requirement` is a **demand** keyed by `(store, sku)` — many candidate rows describe
  the *same* store demand via different hubs.

The algorithm's whole job is to match this shared supply to this competing demand.

---

## Part 4 — The priority rules (tiers and tie-breaks)

### The tier order (the outer priority)

```python
TIERS = [
    (1, 1, BUCKET_IN30),    # dead hub, expiring soon   <- drain first
    (2, 0, BUCKET_IN30),    # live hub, expiring soon
    (3, 1, BUCKET_POST30),  # dead hub, long-dated
    (4, 0, BUCKET_POST30),  # live hub, long-dated      <- drain last
]
```

Read top to bottom = most-preferred to least-preferred inventory. The logic:
**expiring stock is the priority axis** (tiers 1–2 before 3–4), and within each expiry
level we prefer to empty **dead hubs** before live ones. We completely exhaust tier 1
before touching tier 2, and so on.

### Inside a tier (the inner priority)

Two more ordering rules operate inside each tier:

1. **Which hub does a store pick?** Each open store picks the **single candidate MH with
   the most inventory** (`pick_w` — see below). Rationale: the hub with the most stock is
   the one most likely to be able to cover the store's *entire* requirement in one shot
   (remember, it's all-or-nothing).
2. **When a hub is over-subscribed, who wins?** Stores wanting the same hub are stacked
   **largest requirement first** (`cum_w`). We walk down that list accumulating demand and
   keep allocating in full until the next store would overflow the hub; everyone past that
   point gets nothing **this round** and falls through to try another hub next round.

---

## Part 5 — Walking through the code

### 5.1 Configuration (lines 47–63)

```python
INPUT_TABLE     = "gold.scratch.to_network_base"
OUT_LINE_TABLE  = "gold.scratch.to_network_base_allocated"
OUT_FINAL_TABLE = "gold.scratch.to_network_allocation"
```

Where to read the candidate table and where to write the two results. `MAX_ROUNDS = 200`
is a **safety valve**: even if some logic bug prevented the working set from shrinking, the
loop can never spin forever.

### 5.2 `_standardize(df)` (lines 67–80)

A defensive `select` that pins down **exactly** the columns we expect and casts each to a
known type (`store_id` → string, `inv` → double, etc.). This protects the algorithm from
upstream surprises (an int where we expected a string, extra columns, etc.) and documents
the contract in code. Nothing clever — just hygiene.

### 5.3 `allocate(lines)` — the engine (lines 83–264)

This is deliberately separated from any table read/write so it can be **unit-tested** with
a small in-memory DataFrame. `main()` does the I/O; `allocate()` does the thinking.

#### Setup: eligibility and the PO filter (lines 88–94)

```python
lines = lines.localCheckpoint()

po_ok = (F.col("po_base_validation_status").isNull()
         | (F.lower("po_base_validation_status") != F.lit("open")))

eligible_lines = lines.filter((F.col("frequency") == 1) & po_ok).localCheckpoint()
```

- `po_ok` is a reusable boolean expression: "the PO is *not* open" (null counts as OK).
- `eligible_lines` is the subset that can actually receive allocation: it must be
  **scheduled** (`frequency == 1`) and **not PO-open**. Everything else is carried for
  reporting but never wins inventory.
- Both are checkpointed so they're computed once and reused cheaply.

#### Initial state: three DataFrames that evolve (lines 96–117)

The greedy loop mutates three pieces of state. Each is a DataFrame:

**1. `pool_state` — the shared inventory, the thing we drain.**
```python
pool_state = lines.groupBy("mh_id", "sku", "inv_bucket").agg(F.max("inv").alias("inv_balance"))
```
One row per `(mh, sku, bucket)` with its current balance. We use `max("inv")` (not sum)
because `inv` is *repeated* across the store rows — they all carry the same shared number,
so the max is simply that number, de-duplicated. This is the single source of truth for
"how much is left", and it shrinks every round.

**2. `store_state` — the demand and the outcome, the thing we close out.**
```python
store_state = lines.groupBy("store_id", "sku").agg(... balance_req ...)
              .withColumn("done", 0)
              .withColumn("final_mh_id", None) ...
```
One row per `(store, sku)`: how much it still needs (`balance_req`), whether it's finished
(`done`), and — once served — *which* hub/bucket/tier/round served it and how much it got.
At the start every store is open (`done = 0`) and unassigned.

**3. `used_lines` — the memory of "edges we already tried".**
```python
used_lines = eligible_lines.select(keys).limit(0).withColumn("allocation_round", 0)
```
Starts **empty** (`limit(0)` = take zero rows but keep the schema). Every round we add
every `(store, sku, mh, bucket)` edge we *tested*, whether or not it won. This is what
guarantees forward progress: a store that wasn't served by hub A this round won't re-pick
hub A next round — that edge is "used", so it falls through to its next-best hub. Without
this, a store could keep re-picking the same full hub forever.

#### Two window functions: the heart of the priority logic (lines 119–122)

Window functions let you rank/aggregate *within groups* without collapsing rows.

```python
pick_w = Window.partitionBy("store_id", "sku")
               .orderBy(F.col("inv_balance").desc(), F.col("mh_id"))
```
"Within each store × SKU, order its candidate hubs by inventory, biggest first" (hub id
breaks ties for determinism). We'll take rank #1 → the store's chosen hub.

```python
cum_w = Window.partitionBy("mh_id", "sku", "inv_bucket")
              .orderBy(F.col("balance_req").desc(), F.col("store_id"))
              .rowsBetween(unboundedPreceding, currentRow)
```
"Within each hub × SKU × bucket, order the competing stores by requirement, biggest first,
and form a **running cumulative sum** from the top down to the current row." This running
total is how we gate the all-or-nothing fill.

#### The double loop (lines 124–195)

```python
rnd = 0
for tier_no, dead_val, bucket in TIERS:          # OUTER: priority tiers
    tier_lines = eligible_lines.filter(dead_mh == dead_val & inv_bucket == bucket)
    while rnd < MAX_ROUNDS:                       # INNER: rounds within the tier
        ...
```

**Outer loop**: process tiers strictly in priority order. `tier_lines` is just the eligible
edges belonging to the current tier.

**Inner loop** — one *round* does five things:

**(a) Build the working set (lines 131–138).** Join three live facts together:
```python
work = tier_lines
       .join(pool_state.filter(inv_balance > 0), keys)      # only hubs with stock left
       .join(store_state.filter(done==0 & balance_req>0), keys)  # only still-open stores
       .join(used_lines, keys, "left_anti")                 # drop already-tried edges
```
A `left_anti` join means "keep rows from the left that have **no** match on the right" — i.e.
"exclude edges already in `used_lines`." After this, `work` = every edge that is still
genuinely in play this round.

**(b) Pick one hub per store (line 141).**
```python
cand = work.withColumn("rn", row_number().over(pick_w)).filter(rn == 1)
```
Rank each store's candidate hubs by inventory and keep only #1. Now every open store
appears at most once — its single chosen hub for this round.

**(c) Gate by cumulative demand, all-or-nothing (lines 144–151).**
```python
gated = cand.withColumn("cum_req", F.sum("balance_req").over(cum_w))
            .withColumn("allocated_qty",
                F.when(cum_req <= inv_balance, balance_req).otherwise(0.0))
```
For each hub, stores are stacked biggest-first; `cum_req` is the running total of demand
from the top. A store gets its **full** requirement *only if* the cumulative demand up to
and including it still fits inside the hub's balance; otherwise it gets `0` and will try a
different hub next round. This is exactly the "fill biggest customers until the next one
won't fit" rule, expressed without any row-by-row looping.

**(d) Termination check (lines 153–154).**
```python
if gated.count() == 0:
    break        # nothing left to test in this tier -> move to next tier
```
When the working set is empty (no open stores can reach any hub-with-stock via an untried
edge), the tier is done.

**(e) Update the three states (lines 156–195).**
- **Pool**: subtract each hub's allocated total from its balance (`pool_state` shrinks).
- **Store**: any store with `allocated_qty > 0` is a *winner* — mark `done = 1`, stamp its
  `final_mh_id`, `final_bucket`, `final_tier`, `final_round`, set its allocated quantity and
  zero its `balance_req`. (`coalesce` is used so we only ever stamp the **first** win and
  never overwrite it.)
- **Used**: append *every* edge tested this round (winners and losers alike) to
  `used_lines`, so losers fall through next round.

Then `rnd += 1` and we loop. Each round the pool can only shrink and `used_lines` can only
grow, so the working set strictly shrinks → the loop is guaranteed to end.

> **The whole greedy algorithm in one breath:** for each tier in priority order, repeatedly
> let every still-open store grab its highest-inventory untried hub, fill the biggest
> requirements first until the hub can't fully cover the next store, subtract what was
> taken, close the served stores, remember every edge tried, and repeat until nothing in
> the tier can move — then go to the next tier.

### 5.4 Building the outputs (lines 197–264)

After the loop, `pool_state` holds final remaining inventory, `store_state` holds each
store's outcome, and `used_lines` holds the full audit trail of attempts. Two tables are
assembled:

**Line-level output `line_out` (lines 209–232).** Starts from **all** original input lines
(nothing dropped — full transparency) and left-joins on:
- `eligible` flag,
- `allocation_round` (when this edge was tried),
- `allocated_qty` (what the winning edge received; 0 elsewhere),
- `total_allocated` (total drained from that hub×sku×bucket),
- `inv_remaining` (final pool balance).

This is the granular, RCA-friendly view: for any candidate edge you can see whether it was
eligible, whether/when it was tried, what it got, and the hub's end state.

**Store × SKU final output `final_out` (lines 234–262).** One row per store × SKU — the
decision table the business consumes:
- `final_mh_id`, `final_bucket`, `final_tier`, `final_round` — who served it and when,
- `allocated_qty` and `balance_requirement` (what remained unmet),
- `n_candidate_mhs` — how many real candidate hubs it had (with stock),
- a human-readable **`reason`**:

```python
reason = NO_REQUIREMENT  if ds_requirement <= 0       # nothing was needed
       = ALLOCATED       if final_mh_id is set         # served
       = UNALLOCATED     if it had candidates but lost  # competed and lost / hubs emptied
       = SKIP_PO_OPEN    if its only lines were PO-open  # blocked by open PO
       = NO_CANDIDATE    otherwise                       # no eligible hub at all
```

`reason` is the single most useful diagnostic column: it tells planners *why* any store
ended up where it did, which is the first question they always ask.

### 5.5 `main()` — the I/O wrapper (lines 267–283)

```python
spark = SparkSession.builder.getOrCreate()
lines = _standardize(spark.table(INPUT_TABLE))
line_out, final_out = allocate(lines)
line_out.write... saveAsTable(OUT_LINE_TABLE)
final_out.write... saveAsTable(OUT_FINAL_TABLE)
```

Read the candidate table, standardise it, run the engine, write both results as Delta
tables (overwriting, with schema overwrite so column changes don't break the run). The
separation means the pure logic in `allocate()` can be tested without a warehouse.

---

## Part 6 — Why the design choices are the way they are (recap)

| Choice | Reason |
|---|---|
| **Greedy, tier-by-tier** | Matches the business priority (drain expiring/dead stock first) and how a planner would manually do it; explainable. |
| **One MH per store × SKU** | Operational: a store shouldn't receive one SKU from many hubs (many trucks, many receipts). |
| **All-or-nothing fill** | A partial transfer that doesn't cover the store's need still costs a full truck for little benefit; better to send the store fully elsewhere. |
| **Highest-inventory hub picked first** | The hub most likely to cover the *whole* requirement in one shot. |
| **Biggest requirement first within a hub** | Serve the stores that move the most volume; avoids stranding a big store behind many tiny ones. |
| **`used_lines` accumulator** | Guarantees forward progress — losers fall through to their next-best hub instead of re-picking a full one. |
| **`localCheckpoint()` every round** | Cuts Spark's lineage so an iterative loop doesn't blow up the query plan; the *enabling* trick for greedy-in-Spark at 20M+ scale. |
| **`max("inv")` for the pool** | `inv` repeats across store rows (shared pool), so max de-duplicates it correctly. |
| **`coalesce` when stamping winners** | Records only the *first* win per store and never overwrites it. |
| **All input lines kept in `line_out`** | Full auditability / RCA — you can always explain any edge. |
| **`MAX_ROUNDS` guard** | Hard stop against any pathological non-terminating case. |

---

## Part 7 — A tiny worked example

Say SKU `A`, expiring-soon bucket, one dead hub `H1` with **100** units. Three stores want
`A`: S1 needs 60, S2 needs 50, S3 needs 30. All three list `H1` as their best hub.

**Tier 1, round 1:**
- Each store picks `H1` (only/highest-inventory hub). 
- Stack biggest-first: S1(60), S2(50), S3(30). Cumulative: 60, 110, 140.
- Gate vs 100: S1 cum=60 ≤ 100 → gets **60**. S2 cum=110 > 100 → gets **0**. S3 cum=140 > 100 → gets **0**.
- Pool: 100 − 60 = **40** left. S1 closed (`final_mh_id=H1`). All three edges marked used.

**Round 2:**
- S1 is done. S2 and S3 re-enter, but their `H1` edge is now *used*, so they fall to their
  next-best hub (if any). Suppose S3 also had hub `H2`… it now competes there. S2 with no
  other hub stays open.
- …and so on, until no open store can reach any hub-with-stock via an untried edge, then
  tier 1 ends and tier 2 begins.

S2 ends up `UNALLOCATED` (it had a candidate but lost the competition and had no fallback);
S1 and S3 end `ALLOCATED`. That's exactly the kind of explainable outcome the `reason`
column surfaces.

---

## Glossary

- **MH / mother hub** — supplying warehouse.
- **TO** — transfer order (move stock hub → store).
- **SKU** — a specific product.
- **inv_bucket** — expiry grouping of inventory (`expiry_in_30days` vs `expiry_post_30days`).
- **dead_mh** — a hub being wound down; we prefer to empty it.
- **frequency = 1** — the hub is scheduled to ship to that store tomorrow (a hard
  eligibility gate).
- **PO open** — unresolved purchase order; such lines are skipped.
- **greedy** — make the locally best choice each step, never backtrack.
- **DataFrame** — Spark's distributed table.
- **lazy / action** — Spark builds a recipe and only computes on an action.
- **localCheckpoint** — force-compute now and forget the lineage (keeps loops sane).
- **window function** — rank/aggregate within groups without collapsing rows.
- **left_anti join** — keep left rows that have *no* match on the right (used here to
  exclude already-tried edges).
