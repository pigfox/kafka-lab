# PF-S313 — predictions, registered BEFORE the graded run

Written and committed before either arm was started. Nothing below was edited
after seeing a result; the actuals live in `results.md` beside the raw captures.

## The run

| parameter | value |
|---|---|
| fault rate (`KL_FAULT_RATE`) | `0.01` |
| fault seed (`KL_FAULT_SEED`) | `pf-s313` |
| run nonce (`KL_RUN_NONCE`) | `pf-s313-graded` |
| event seed (`KL_EVENT_SEED`) | `1` (default) |
| partitions | `3` (default) |
| dedupe capacity / TTL | `50000` / `10m` (defaults) |
| arms | `KL_DEDUPE=false`, then `KL_DEDUPE=true` |
| between arms | `./stop.sh` — containers and their storage removed, so the second arm is not fed the first's offsets |
| compose project | `kafka-lab`, exported explicitly on every command |

Both arms are fed BYTE-IDENTICAL RECORD KEYS. The event generator is seeded and
the run nonce is pinned, so record *n* of either arm carries the same identity;
and the fault decision is `sha256(seed ‖ key)`, so the same keys imply the same
faults. Without the pinned nonce each producer start mints a fresh one, the two
arms would have disjoint key spaces, and no fault-set comparison would be
possible at all.

## Predicted — invariants

| # | arm | prediction | reason |
|---|---|---|---|
| 1 | off | `double_applied_total > 0` | every rewind redelivers records whose effect already ran, and nothing suppresses them |
| 2 | off | `duplicates_suppressed_total == 0` | the seen-set is never consulted when dedupe is off |
| 3 | off | `dedupe_evictions_total == 0` (both stores) | ~5k distinct keys against a 50 000-key capacity |
| 4 | off | `dedupe_expiries_total == 0` (both stores) | the arm runs for ~2 minutes against a 10-minute TTL |
| 5 | on | `double_applied_total == 0` | every redelivery is suppressed before it can reach the apply tally |
| 6 | on | `duplicates_suppressed_total > 0` | the same faults fire, so the same redeliveries arrive |
| 7 | on | `dedupe_evictions_total == 0` (both stores) | same headroom as arm off |
| 8 | on | `dedupe_expiries_total == 0` (both stores) | same window as arm off |

## Predicted — exact equalities

| # | prediction | reason |
|---|---|---|
| 9 | the two arms' fault sets are identical over the records BOTH arms delivered | the fault decision is a pure function of `(seed, key)` and both arms see identical keys; equality is asserted over the common prefix because the arms will not consume exactly the same number of records in equal wall time, and a key the shorter arm never reached cannot have faulted in it |
| 10 | off-arm `double_applied_total == redelivery lines in the off-arm log` | every redelivered record with an already-applied key is one double apply and one log line |
| 11 | on-arm `duplicates_suppressed_total == redelivery lines in the on-arm log` | with zero evictions and zero expiries, every redelivered record's key is still remembered, so every one is suppressed and logged |
| 12 | off-arm `applied_total == consumed_total` | with dedupe off nothing is suppressed, so every record the loop passes runs its effect |
| 13 | on-arm `applied_total == distinct keys delivered on that arm` | each distinct key applies exactly once; every repeat is suppressed |

## One prediction from the directive that is NOT registered, and why

The directive asked to register:

> EXACT: off-arm applied − consumed == redelivered count from the log

**That cannot hold, and it is not a close call.** `consumed_total` is incremented
by `drainCounter` for EVERY record the loop finishes — the PF-S311 item-4 ruling,
upheld in this directive, that `consumed_total` counts the loop and must not
move. `applied_total` is incremented for every record whose effect runs. On the
off arm nothing is suppressed, so those two counters increment on exactly the
same events and `applied − consumed == 0` by construction, redeliveries included:
a redelivered record is counted in BOTH.

So prediction 12 above (`applied == consumed`, which the directive also asks for)
and the subtraction identity can only both hold if the redelivered count is zero
— which D6 explicitly forbids. The two are in direct contradiction.

The measurement the subtraction was reaching for is prediction 10:
`double_applied_total == redelivered count`. That is registered above, and it is
exact.

## What would make this run unpublishable

- Any non-zero eviction or expiry on the dedupe arm (predictions 7 and 8). The
  number would then be measuring the size of the store rather than the delivery
  semantics. Raising the capacity to clear it is not a remedy — it is the same
  run with the evidence removed.
- A zero `double_applied_total` on the off arm (prediction 1). That means the
  injector does not produce redeliveries, and every other figure here is
  measuring nothing.

## Not predicted, deliberately

**The exact value of `double_applied_total`.** A rewind moves a PARTITION cursor,
so it redelivers the faulted record and every later record of that partition
already polled. The multiplier is therefore batch composition, which the broker
decides and which is not reproducible across two separate lab runs. Registering
an integer here would be registering a number known in advance to be unhittable.

The dry run measured 4 faults producing 18 redeliveries — a multiplier of about
4.5 — which is reported as an observation, not a prediction.
