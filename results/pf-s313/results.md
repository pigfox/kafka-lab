# PF-S313 — the graded run

Predictions were registered in [`predictions.md`](predictions.md) and committed
as `ce9110c`, before either arm was started. Raw captures are in
[`capture/`](capture/), unedited.

**Two runs happened. Run 2 is the published one, and run 1 is kept** in
[`capture-run1/`](capture-run1/) because a prediction failed against it and the
failure was informative. See the section at the end.

## Actuals — run 2

| | at-least-once (`KL_DEDUPE=false`) | idempotent (`KL_DEDUPE=true`) |
|---|---|---|
| records the loop finished (`consumed_total`) | 6773 | 7515 |
| effects run (`applied_total`) | 6773 | 5389 |
| **applied a second time** (`double_applied_total`) | **1948** | **0** |
| duplicates suppressed | 0 | 2126 |
| records with no dedupe key | 0 | 0 |
| evictions (`seen` / `applied`) | 0 / 0 | 0 / 0 |
| expiries (`seen` / `applied`) | 0 / 0 | 0 / 0 |
| keys fired | 56 | 59 |
| cursor rewinds | 27 | 36 |
| redeliveries logged | 1948 | 2126 |

## Predictions against actuals

| # | prediction | actual | verdict |
|---|---|---|---|
| 1 | off: `double_applied > 0` | 1948 | **hit** |
| 2 | off: `suppressed == 0` | 0 | **hit** |
| 3 | off: evictions 0 | 0 / 0 | **hit** |
| 4 | off: expiries 0 | 0 / 0 | **hit** |
| 5 | on: `double_applied == 0` | 0 | **hit** |
| 6 | on: `suppressed > 0` | 2126 | **hit** |
| 7 | on: evictions 0 | 0 / 0 | **hit** |
| 8 | on: expiries 0 | 0 / 0 | **hit** |
| 9 | both arms fired the identical fault set over the records both delivered | 54 keys across 3 partitions, identical | **hit — but only after the comparison window was corrected twice; see below** |
| 10 | off: `double_applied == redeliveries logged` | 1948 == 1948 | **hit, exact** |
| 11 | on: `suppressed == redeliveries logged` | 2126 == 2126 | **hit, exact** |
| 12 | off: `applied == consumed` | 6773 == 6773 | **hit, exact** |
| 13 | on: `applied == distinct keys delivered` | 5389 applied + 2126 suppressed == 7515 consumed | **hit, exact** |

Thirteen registered, thirteen hit. Prediction 9 is the one that took work, and
it is the one worth reading about.

## What 1948 duplicates from 56 faults means

**Roughly 35 duplicate applications per injected fault.** A rewind moves a
PARTITION cursor, so it replays the faulted record and every later record of that
partition the loop had already polled. Duplicates therefore scale with batch
size, not with the fault rate.

Two figures make that concrete. 56 keys fired but only 27 rewinds happened, on
the at-least-once arm: `Targets` returns at most one record per partition per
batch, so eligible keys sharing a partition and a batch are spent together. And
the multiplier is not stable — the sizing run measured 4 faults producing 18
duplicates, about 4.5×, because it ran for a shorter window with smaller batches.
As lag builds the consumer polls larger batches, so each rewind replays more.

Anyone sizing a system on "we lose 1% of commits, so we expect about 1%
duplicates" is wrong by more than an order of magnitude.

## Prediction 9 and the two corrections it needed

**Run 1 failed it, for a real reason.** The consumer logged the REWIND TARGETS
and called that the fault record. Targets returns at most one record per
partition per batch, so the target set is a subset of the fired set chosen by
broker batching — the two arms shared only 25 of their 34 and 30 logged keys.
Recomputing eligibility directly confirmed the predicate itself was never at
fault: every key checked from the "one arm only" lists is eligible under the
seed.

The fix was to report every key that FIRES, through a callback, and log it
separately from the rewind targets. The fired set is a pure function of the seed
and the delivered keys; the target set is not. Run 2 was then run from scratch.

**The comparison window then needed correcting too, and this was my error rather
than the code's.** The natural window — "sequence numbers up to the smaller arm's
highest" — is wrong at both ends, and run 2 showed both:

- The consumer joins the group at the END of the topic, so each arm starts
  wherever the producer had reached. The at-least-once arm's first fired key was
  seq 474; the dedupe arm's was seq 251. Keys 251, 331 and 350 were never
  DELIVERED to the first arm.
- The sequence number is global but there are three partitions, sitting at
  different offsets at any instant. The dedupe arm had fired at seq 6106 and had
  not yet been delivered seq 5821.

A global sequence number is not a watermark. The sound window is a range of
OFFSETS WITHIN ONE PARTITION, because offsets are contiguous and consumed in
order. Under that window the fault sets are identical: 54 keys, three
partitions, no difference in either direction.

## One prediction from the directive that was not registered

The directive asked for `off-arm applied − consumed == redelivered count`. It
cannot hold: `consumed_total` counts every record the loop finishes and
`applied_total` counts every effect that runs, so with nothing suppressed both
increment on the same events and the difference is zero by construction.
It also contradicts the directive's own adjacent prediction 12
(`applied == consumed`), and the two can only agree if the redelivered count is
zero — which D6 forbids. `predictions.md` records this in full. The measurement
it was reaching for was registered instead as prediction 10, and it is exact.

## Reproducing

```sh
COMPOSE_PROJECT_NAME=kafka-lab ./stop.sh
COMPOSE_PROJECT_NAME=kafka-lab KL_FAULT_RATE=0.01 KL_FAULT_SEED=pf-s313 \
  KL_RUN_NONCE=pf-s313-graded KL_DEDUPE=false ./run.sh
# wait, scrape http://localhost:2112/metrics inside the consumer, capture logs
COMPOSE_PROJECT_NAME=kafka-lab ./stop.sh
COMPOSE_PROJECT_NAME=kafka-lab KL_FAULT_RATE=0.01 KL_FAULT_SEED=pf-s313 \
  KL_RUN_NONCE=pf-s313-graded KL_DEDUPE=true ./run.sh
```

`COMPOSE_PROJECT_NAME` is exported on every command deliberately: the ambient
value on the machine this was run on is `pf`, which `run.sh` overrides for
itself but a bare `docker compose` does not.

The counts will not reproduce exactly — batch composition is the broker's
decision and the arms do not run for identical wall time. What does reproduce is
the fault set, and every invariant in the table above.
