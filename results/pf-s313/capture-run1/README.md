# Run 1 — kept as evidence, NOT the published run

These captures are real and unedited. They are kept because prediction 9 —
"the two arms' fault sets are identical" — FAILED against them, and the failure
was informative rather than noise.

## What they show

| | arm off | arm on |
|---|---|---|
| logged fault keys | 34 | 30 |
| shared between the arms | 25 | 25 |
| in that arm only | 9 | 5 |

## Why

The consumer logged the REWIND TARGETS and called that the fault record.
`fault.Injector.Targets` returns at most ONE record per partition per batch, so
when two eligible keys land in the same partition of the same batch, both are
marked fired and only the earlier is returned — the later is spent silently.

Recomputing eligibility directly confirmed the predicate was never at fault:
all four keys checked from the two "in that arm only" lists
(`:1412`, `:715`, `:2540`, `:5524`) are eligible under seed `pf-s313` at rate
`0.01`, and 66 of seq 1..6000 are eligible in total. Both arms fired far more
keys than either logged; they simply logged different subsets, and the subset
was chosen by broker batch composition.

So the FIRED set is a pure function of the seed and the delivered keys, and the
TARGET set is a subset of it decided by batching. Only the first is
reproducible, and only the first is evidence.

## What changed as a result

`fault.New` gained an `onFault` callback invoked for every key that fires, and
the consumer logs each one as `msg="fault fired"`. The rewind targets are still
logged, by the consume loop, as `msg="rewinding instead of committing"` — they
are worth seeing, they are just not the fault record.

`TestEveryFiredKeyIsReportedThroughTheCallback` and
`TestTheFiredSetSurvivesADifferentBatchShape` pin the new behaviour, and
`TestTheTargetSetDoesMoveWithBatchShape` pins the counterpart so the first two
cannot pass because the target set happened to be stable as well.

Run 2, in `../capture/`, is the published run.
