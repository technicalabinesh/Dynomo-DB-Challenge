# Virtual Waiting Room — DynamoDB Data Modeling Challenge

A design for fairly queuing up to 10M concurrent fans, assigning verifiable queue
positions, and progressively promoting them into a purchase flow without
overwhelming downstream ticketing systems.

---

## 1. Table Design

### Design philosophy

The hard part of this problem isn't storing a fan record — it's that DynamoDB has
**no native global ordering/sort across a whole table**. A "queue position" implies
a total order over up to 10M items written in a few seconds. We get that order by
**sharding writes** (so no single partition becomes a hot spot during the stampede)
and **deriving global rank cheaply from a monotonic key**, rather than trying to
compute a live rank number at write time.

### Single-table design

**Table: `WaitingRoom`**

| Attribute | Type | Notes |
|---|---|---|
| `PK` | S | `EVENT#<eventId>#SHARD#<shardId>` |
| `SK` | S | `FAN#<sequenceToken>` — see §2 for how this is built |
| `fanId` | S | Cognito/user identity |
| `entryTimestampMs` | N | Server-side arrival time (epoch ms) |
| `clientNonce` | S | Random tie-breaker generated client-side, echoed back for verification |
| `shardId` | N | 0–N, assigned by hashing `fanId` |
| `globalSeq` | N | Monotonic sequence *within the shard*, from a sharded counter |
| `eligibilityStatus` | S | `WAITING` \| `ELIGIBLE` \| `EXPIRED` \| `PURCHASING` \| `DONE` |
| `batchId` | S | Set at promotion time |
| `promotedAtMs` | N | Set at promotion time |
| `eligibilityExpiresAtMs` | N | TTL-adjacent field for the purchase window |
| `verificationHash` | S | HMAC of `(fanId, shardId, globalSeq, entryTimestampMs)` — prevents position spoofing |
| `ttl` | N | DynamoDB native TTL attribute for auto-expiry of stale/abandoned records |

**Partition key strategy:** the naive `PK = EVENT#<eventId>` would put all 10M
writes on one logical partition during the stampede. Instead we pre-split into
`N` shards (e.g., 200–500, tuned so each shard absorbs roughly 20K–50K writes/sec
peak, well under DynamoDB's per-partition ~1,000 WCU practical ceiling). Fans are
assigned a shard by `hash(fanId or connectionId) % N`, which spreads writes evenly
and gives predictable per-partition throughput.

### Global Secondary Indexes

**GSI1 — `StatusIndex`** (for batch promotion queries and admin dashboards)
- `GSI1PK = EVENT#<eventId>#STATUS#<eligibilityStatus>`
- `GSI1SK = SHARD#<shardId>#SEQ#<globalSeq>`

Lets the promotion engine efficiently query "give me the next N `WAITING` fans in
shard order" and lets ops query "how many fans are currently `ELIGIBLE`."

**GSI2 — `FanLookupIndex`** (for fan self-service status checks)
- `GSI2PK = FAN#<fanId>#EVENT#<eventId>`
- `GSI2SK = ENTRY#<entryTimestampMs>`

Lets a fan's own client look up their record by identity without knowing their
shard — a single-item `Query`, strongly consistent optional, cheap.

Both GSIs are **sparse-friendly** and provisioned with on-demand or auto-scaled
capacity, since their access pattern is bursty but much lower volume than the
initial write stampede (reads dominate; writes to the base table are what spike).

### Billing mode

**On-demand capacity** for the initial launch window (the stampede is a known,
short, extreme spike — provisioning fixed WCUs for a once-a-quarter 10M-write
burst is wasteful and risky if under-provisioned). Optionally pre-warm with a
short burst of dummy traffic 60–90 seconds before doors-open if using
provisioned + auto-scaling instead, since auto-scaling reacts with a lag on-demand
doesn't have.

---

## 2. Fair Queue Position Assignment

### The core problem

At "go time," up to 10M fans hit an API Gateway/Lambda (or Fargate) fleet within
seconds. We need a queue position that is:
- **Fair** — first-arrived, first-served, without allowing manipulation.
- **Collision-free** — no two fans get the same rank.
- **Cheap to assign** — no single hot key (a naive `UpdateItem` atomic counter on
  one item caps out around ~1,000 updates/sec on that item, far too slow).
- **Tolerant of clock skew** — client and even server clocks are never perfectly
  synchronized at millisecond granularity across a fleet.

### Approach: sharded monotonic counters + composite ordering key

1. **Ingestion tier is stateless.** An API Gateway + Lambda (or Fargate behind an
   ALB) fleet accepts the "join queue" request. It does **not** wait on a single
   global counter.
2. **Shard assignment.** Each request is hashed to one of `N` shards based on a
   random client-generated `connectionId` (not `fanId`, to avoid any fan-level
   hot-spotting if a single account somehow floods requests).
3. **Per-shard sequence number.** Each shard has its own atomic counter, stored as
   a single item (`PK = EVENT#<id>#COUNTER#<shardId>`) updated via
   `UpdateItem ... ADD globalSeq :incr` with a conditional expression. With `N=300`
   shards absorbing a combined 10M writes in ~10 seconds, each shard only needs to
   sustain ~3,300 increments/sec — comfortably within a single item's write
   throughput when using DynamoDB's built-in **atomic counter sharding**
   (further split each shard's counter into e.g. 5 sub-counters if needed, and sum
   them at promotion time).
4. **Composite ordering key.** A fan's *effective global rank* is never computed
   at write time — it's derived lazily as `(shardId, globalSeq)`, and true
   cross-shard chronological order is approximated by **server-received
   timestamp**, not client clock, recorded once at the point the request hits the
   Lambda handler (`entryTimestampMs`). This sidesteps client clock skew entirely:
   we never trust the client's notion of "when" beyond using it as a UI hint.
5. **Tie-breaking.** Within the same millisecond on the same shard (very possible
   at this volume), the atomic counter's `globalSeq` is the true tie-breaker — it's
   assigned by DynamoDB's serialized conditional update, so it can never collide,
   regardless of how many requests land in the same millisecond.
6. **Anti-gaming.** Each fan record is stamped with
   `verificationHash = HMAC_SHA256(secret, fanId|shardId|globalSeq|entryTimestampMs)`.
   The fan's client displays and can be asked to re-present this hash; the server
   never accepts a client-supplied queue position, shard, or sequence number —
   only the value it wrote itself. This defeats attempts to fabricate a low
   position via forged API calls or replay.
7. **Bounding the stampede at the edge.** Before any of this hits DynamoDB, a CDN
   or API Gateway usage plan applies coarse admission control (e.g., a Lambda@Edge
   or WAF rate-based rule) so genuinely malicious multi-account bot floods are
   throttled before they can consume shard-counter throughput meant for real fans.

### Why not a single global sequence?

A single `ADD seq :1` on one item is simple and *perfectly* ordered, but hard-caps
throughput at roughly 1,000 updates/sec for that item (DynamoDB partition write
limits), which would take **~3 hours** to admit 10M fans at peak — unacceptable.
Sharding trades perfect global ordering for "ordering within a shard + fair
round-robin across shards at promotion time" (see §3), which is indistinguishable
from perfect fairness to the end user as long as promotion interleaves shards
proportionally.

---

## 3. Batch Promotion Strategy

### Goal

Move fans from `WAITING` → `ELIGIBLE` in controlled batches that match the
downstream purchase system's real capacity, without ever exceeding it, and
without starving any shard (so no fan feels "stuck" behind a slow shard).

### Mechanics

1. **Promotion is driven by a control-plane process**, not by fans polling their
   own status — a scheduled EventBridge rule (e.g., every 2–5 seconds) invokes a
   `PromotionOrchestrator` Lambda.
2. **Capacity signal.** The orchestrator reads current downstream capacity from a
   lightweight `PurchaseCapacity` item (`activePurchasers`, `maxConcurrentPurchasers`,
   `avgCheckoutDurationMs`) maintained by the checkout service itself (see §5 for
   the self-balancing stretch goal). `batchSize = maxConcurrentPurchasers -
   activePurchasers`, floored at 0.
3. **Round-robin shard draw.** Rather than draining shard 0 completely before
   touching shard 1 (which would make shard assignment itself feel unfair), the
   orchestrator draws `batchSize / N` fans from each shard's lowest outstanding
   `globalSeq` via `GSI1` (`StatusIndex`, filtered to `WAITING`, ordered by
   `globalSeq`), using `Query` with `Limit` per shard. This approximates true
   global FIFO order across shards proportionally, since all shards fill at
   roughly the same rate during the stampede.
4. **Atomic batch promotion.** Selected fans are transitioned via
   `TransactWriteItems` in chunks of 25 (DynamoDB transaction limit), each write
   conditioned on `eligibilityStatus = WAITING` — this makes promotion
   idempotent and safe against the orchestrator retrying or double-firing.
5. **Overrun prevention.** Each promoted fan gets `eligibilityExpiresAtMs = now +
   purchaseWindowMs` (e.g., 10 minutes). If a fan doesn't complete checkout in
   that window, their status flips to `EXPIRED` (via TTL-driven cleanup or a
   sweep Lambda) and their purchase slot is released back to
   `PurchaseCapacity.activePurchasers`, which lets the *next* promotion cycle pull
   a fresh batch in — this is exactly the stretch-goal mechanism in §5.
6. **Never overshoot capacity:** because `batchSize` is recomputed from live
   capacity every cycle (not a fixed schedule of "promote 1,000 every minute"),
   a slow checkout period naturally throttles promotions, and a fast one
   accelerates them — the queue self-regulates against actual downstream load
   rather than a guessed static rate.

---

## 4. Fan Status Query Design

### Requirement

Millions of fans polling "where am I in line?" simultaneously must not create a
second stampede on the table.

### Design

1. **Every status check is a single-item or single-partition `Query` on
   `GSI2` (`FanLookupIndex`)** keyed by the fan's own identity — never a scan,
   never a cross-shard aggregation at read time.
2. **Eventually consistent reads** are used for status polling (default, half the
   cost of strongly consistent, and a queue position lagging by tens of
   milliseconds is imperceptible to a human).
3. **DAX (DynamoDB Accelerator)** sits in front of the table for this specific
   access pattern. Status polling is extremely read-heavy and highly repetitive
   (the same item requested every 3–5 seconds by the same fan) — a textbook DAX
   cache case, absorbing the bulk of read traffic with microsecond latency and
   protecting base-table RCUs for the promotion engine's own reads.
4. **Estimated wait time is *not* computed per-request.** A cheap background
   Lambda (every few seconds) computes, per shard, `estimatedFansAheadOf(seq) =
   (globalSeq - currentPromotedSeq)` and a rolling `avgPromotionRatePerSecond`,
   writing a small denormalized `QueueStats` item. Each fan's status response
   combines their own cached position with this shared stats item — one extra
   cached read, not a live count.
5. **Client-side backoff.** Fans' clients poll on a jittered interval (e.g.,
   3–8s randomized) rather than a fixed interval, and the interval widens the
   further back in the queue they are — this smooths the read pattern instead of
   having all clients poll in lockstep on the second.
6. **Push over pull where possible.** For fans nearing promotion (e.g., top 1,000
   in their shard), the promotion Lambda can optionally publish to a WebSocket
   (API Gateway WebSocket API) or send a mobile push notification, cutting their
   need to poll at all right when it matters most.

---

## 5. Stretch Goal — Maintaining 1,000 Active Purchasers

Extends §3's capacity model into a closed feedback loop:

1. `PurchaseCapacity` item tracks `activePurchasers` via atomic `ADD`/`SUBTRACT`
   as fans enter (`PURCHASING`) and leave (`DONE` or `EXPIRED`) checkout.
2. Every purchase-flow entry/exit event (from the checkout service, via
   EventBridge or DynamoDB Streams on status transitions) triggers a lightweight
   Lambda that adjusts `activePurchasers` and, if it drops below 1,000, **immediately
   invokes the promotion orchestrator out-of-cycle** rather than waiting for the
   next scheduled tick — this keeps the purchasing pool topped up in near
   real-time instead of only refilling on a timer.
3. **DynamoDB Streams on the base table**, filtered to `eligibilityStatus`
   transitions, is the trigger source — this reacts to *actual* completions/
   expirations rather than polling, and scales naturally with checkout volume.
4. A small hysteresis band (e.g., only top up if `activePurchasers < 950`) avoids
   thrashing the orchestrator on every single checkout completion.

---

## Summary of Access Patterns

| Access Pattern | Table/Index | Operation |
|---|---|---|
| Fan joins queue | Base table | `PutItem` (sharded PK) |
| Increment shard sequence | Counter item | `UpdateItem ADD` |
| Fan checks own status | GSI2 (`FanLookupIndex`) | `Query` (cached via DAX) |
| Orchestrator pulls next batch per shard | GSI1 (`StatusIndex`) | `Query` (WAITING, by globalSeq) |
| Promote a batch | Base table | `TransactWriteItems` (conditional) |
| Expire stale eligibility | Base table (TTL) | Native TTL deletion / sweep |
| Track live capacity | `PurchaseCapacity` item | `UpdateItem ADD` via Streams trigger |
| Compute wait-time estimate | `QueueStats` item | Background Lambda write, cached read |

This design keeps every hot-path write sharded and conditional, every hot-path
read cached or single-partition, and uses DynamoDB Streams + EventBridge to turn
promotion from a fixed schedule into a live feedback loop against real downstream
capacity.
