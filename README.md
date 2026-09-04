# kafka-lab

A Kafka backpressure lab you can watch. Throttle the consumer below the
producer and see lag climb; open it back up and see it drain.

Everything runs locally in Docker. There is no cloud account, no `.env` file, no
build step and no configuration to write.

```sh
git clone https://github.com/pigfox/kafka-lab
cd kafka-lab
./run.sh
```

Then open **<http://localhost:18080>**.

`./stop.sh` shuts it down and resets it to nothing.

---

## What to click

The whole demo is three drags of one slider.

1. **Open <http://localhost:18080>.** The producer and the consumer both start
   at 50 messages/second, so *consumer group lag* sits at or near zero. The live
   tail scrolls past at the bottom of the page.

2. **Drag the consumer rate down to about 5.** Within a couple of seconds the
   lag figure starts climbing and does not stop. The producer is putting 50
   messages a second into the topic and the consumer is taking 5 out. The `+Nms`
   figure on each tailed message — how long that message waited between being
   produced and being read — grows with it.

3. **Drag the consumer rate back up, past the producer's.** Lag stops climbing,
   turns over, and falls to zero as the consumer works through the backlog. The
   consumed rate briefly exceeds the produced rate; that gap is the drain.

4. **Try the work slider.** Set the consumer rate to 200 and the simulated work
   to 20ms. The consumer now *achieves* about 50/s no matter what the rate
   slider says, because one consumer cannot exceed one message per 20ms however
   fast it is asked to go. The UI shows the requested figure and the achieved
   figure in separate panels, so the difference is visible rather than confusing.

5. **Open Grafana** (<http://localhost:18081>) for the same story as one graph
   over time. It opens straight onto the lab dashboard — no login, nothing to
   configure. The top panel carries produce rate, consume rate and lag together;
   lag has its own axis on the right because it is a count of messages, not a
   rate.

   > **Give it a moment the first time.** Grafana loads its panel plugin lazily,
   > so on the very first dashboard view you will see the panel outlines with
   > *"Loading plugin panel…"* in them for anywhere up to about twenty seconds
   > before the graphs appear. It looks like a broken dashboard and is not one;
   > every later view is instant. This is called out because it fooled the
   > person who built the lab.

6. **Restart the pipeline** and watch the settings survive:

   ```sh
   docker compose restart producer consumer
   ```

   Both come back at the rates you left them at. Nothing was written to a
   database — see *The control plane* below.

---

## Architecture

Eight containers. A Go producer writes synthetic events to the `events` topic; a
Go consumer reads them, sleeps for a configurable simulated work delay, and
commits. Both obey a rate limit. A Go admin service serves the control UI, and
Prometheus, Grafana and kafka-ui provide the observability surface. Kafka runs
as a single broker in **KRaft mode** — no Zookeeper.

The part worth looking at is how the sliders reach the services. **The admin
never calls the producer or the consumer.** It publishes a settings record to a
compacted Kafka topic called `control`; the producer and the consumer tail that
topic and apply what arrives. The demo configures itself over its own bus, which
buys three things that a private HTTP endpoint would not:

- **Settings survive a restart, with no database.** Log compaction keeps the
  latest record for the key. A container that comes back reads the topic from
  the beginning and arrives at the current state.
- **Startup order stops mattering.** A service that boots late reads the
  history. A service that boots early waits for the record.
- **The control path is observable.** Every settings change is a message you can
  read in kafka-ui. An HTTP `PUT` to a private endpoint is not.

The admin's own numbers come from two read-only planes: **Prometheus** for the
achieved produce and consume rates, and the **group coordinator** for consumer
lag. Its live message tail joins a *separate* consumer group from the working
consumer, reading from the latest offset — so opening the UI does not take
partitions from the consumer, and watching the pipeline does not change the
thing being measured.

```
                 ┌──────────────┐
      sliders    │  admin :8080 │  UI + /metrics
   ───────────►  │              │
                 └──┬────────┬──┘
      publishes ────┘        └──── reads lag from the group coordinator,
      settings                     and rates from Prometheus
          │
          ▼
   ┌─────────────────────────────────────────────┐
   │  Kafka (KRaft, single broker)               │
   │                                             │
   │   control  ── compacted, 1 partition        │
   │   events   ── 3 partitions, 10m retention   │
   └───▲─────────────────────────┬───────────────┘
       │ produces                │ consumes
       │                         ▼
  ┌────┴─────┐            ┌─────────────┐
  │ producer │            │  consumer   │
  │          │            │             │
  │ tails ───┴─ control ──┴─── tails    │
  └──────────┘            └─────────────┘
       │                         │
       └──── /metrics ───────────┘
                  │
                  ▼
          Prometheus ──► Grafana
```

### Measurements and settings are kept apart

The lab shows both, and they differ. A consumer *asked for* 200/s that spends
20ms per message *achieves* 50/s. Everywhere those two kinds of number appear —
the metric names, the JSON the UI reads, the panels themselves — they are
labelled and separated, because a panel showing the requested figure under an
"achieved" caption would render perfectly, stay internally consistent, and teach
you something false.

- Metrics ending `_total` are counted. Metrics ending `_rate_limit_per_second`
  or `_work_milliseconds` are dialled.
- The UI's `/api/stats` puts them in different objects: `measured` and
  `requested`.
- The Grafana dashboard puts them on different panels, and the settings panels
  are drawn with dashed lines.

---

## Ports

| Service | URL | Port | Why not the usual one |
|---|---|---|---|
| Control UI (admin) | <http://localhost:18080> | 18080 | 8080 is the most contended port on any developer machine |
| Grafana | <http://localhost:18081> | 18081 | not 3000 |
| Prometheus | <http://localhost:18082> | 18082 | not 9090 |
| kafka-ui | <http://localhost:18083> | 18083 | not 8080 |
| Kafka broker | `localhost:19092` | 19092 | not 9092 |

The block was chosen to sit clear of the conventional ports, so the lab does not
collide with whatever else you already have running. Every port is overridable:

```sh
KL_ADMIN_PORT=28080 KL_GRAFANA_PORT=28081 ./run.sh
```

`run.sh` checks all five before it starts anything and tells you which one is
taken.

---

## Configuration

There is **no `.env` file**, and that is deliberate: a default whose value
depends on an untracked file is a default nobody can reason about from the
repository. Every knob is an environment variable with a literal default,
readable in `docker-compose.yml`.

| Variable | Default | What it does |
|---|---|---|
| `KL_ADMIN_PORT` | `18080` | Published port for the control UI |
| `KL_GRAFANA_PORT` | `18081` | Published port for Grafana |
| `KL_PROMETHEUS_PORT` | `18082` | Published port for Prometheus |
| `KL_KAFKA_UI_PORT` | `18083` | Published port for kafka-ui |
| `KL_KAFKA_PORT` | `19092` | Published port for the broker's external listener |
| `KL_EVENT_PARTITIONS` | `3` | Partitions on the `events` topic |
| `KL_EVENT_SEED` | `1` | PRNG seed — the event stream is deterministic for a seed |
| `KL_EVENT_FILLER_BYTES` | `0` | Pad each event, to run the lab at a larger message size |
| `KL_RATE_WINDOW` | `30s` | `rate()` window the UI's counters use |
| `KL_TAIL_BUFFER` | `64` | Per-browser buffer on the live tail before it drops |
| `KL_LAG_INTERVAL` | `2s` | How often admin asks the coordinator for lag |
| `KL_READY_TIMEOUT` | `120` | How long `run.sh` waits for admin, in seconds |

Delivery semantics — all off by default, so the lab behaves exactly as it always
has until you ask for one. See [the section above](#delivery-semantics-the-duplicate-that-matters).

| Variable | Default | What it does |
|---|---|---|
| `KL_DEDUPE` | `false` | Suppress a redelivery whose key the consumer has already applied |
| `KL_DEDUPE_CAPACITY` | `50000` | Keys the idempotency store holds; beyond this the oldest is forgotten |
| `KL_DEDUPE_TTL` | `10m` | How long a key is remembered, measured from its first sighting |
| `KL_FAULT_RATE` | `0` | Fraction of distinct keys whose commit is replaced by a cursor rewind |
| `KL_FAULT_SEED` | `kafka-lab` | Makes the fault set a pure function of the record keys |
| `KL_RUN_NONCE` | *(random)* | Pins the producer's identity prefix so two runs emit identical keys. **Experiments only** — two runs sharing a nonce also share identities, so a consumer with dedupe on would discard the second as duplicates of the first |

Kafka speaks PLAINTEXT on the compose network. There are no credentials
anywhere in this repository, and nothing here reads a path outside its own
clone.

---

## Development

```sh
go build ./...
go test ./...
go test -race -covermode=set -coverprofile=cover.out ./...
go tool cover -func=cover.out
```

The tests need no broker and no network. Everything with a decision in it —
the rate limiter that wakes when the rate changes mid-wait, the consume loop's
per-record throttle, the SSE tail's drop-rather-than-block policy, the settings
clamp — lives in `internal/` behind small interfaces, and all of those packages
are at 100% statement coverage. What is *not* covered is the franz-go glue in
`internal/kafkabus` and the wiring in `cmd/*/main.go`: a test that mocks a Kafka
client to assert that a file calls a Kafka client proves only that the mock
matches the code. That layer is exercised by running the lab.

### Layout

```
cmd/producer      emits the synthetic stream          (thin wiring)
cmd/consumer      reads, works, commits               (thin wiring)
cmd/admin         control UI, publishes settings      (thin wiring)
cmd/topic-init    one-shot topic creation             (thin wiring)

internal/control     the settings that travel on the bus, and their bounds
internal/ratelimit   a token bucket whose rate can change while you wait on it
internal/runner      the produce and consume loops
internal/event       the synthetic event stream
internal/fanout      one stream to many readers; drops for slow ones
internal/adminui     the HTTP surface and the embedded single-file UI
internal/promquery   a minimal Prometheus instant-query reader
internal/metrics     what each service exposes on /metrics
internal/kafkabus    the franz-go glue — the only package that imports kgo
internal/config      environment reading, with literal defaults
```

---

## Delivery semantics: the duplicate that matters

Kafka gives you **at-least-once**. A record can be delivered twice, and the
delivery that hurts is not a stray extra message — it is the one that arrives
*after its effect was already applied*, because the process died between doing
the work and recording that it had.

The consume loop is arranged to make that window real rather than theoretical:

1. fetch a batch,
2. apply every record in it,
3. commit the offsets.

Stop the process between 2 and 3 and the effects have happened while the offsets
say they have not. Whoever owns those partitions next reads the same records
again. Committing *first* would be worse, not better — the offset would move past
work that never happened, and the messages would be gone instead of repeated.
At-least-once is the choice, and the duplicate is its price.

The lab can inject that failure on demand and count what it costs.

### Try it

```sh
# the at-least-once arm: duplicates land and are applied a second time
KL_FAULT_RATE=0.01 KL_FAULT_SEED=pf-s313 ./run.sh

# the idempotent arm: the same duplicates land and are suppressed
KL_DEDUPE=true KL_FAULT_RATE=0.01 KL_FAULT_SEED=pf-s313 ./run.sh
```

Both are off by default. The Grafana dashboard's last panel plots all four
numbers below.

### The mechanism, and what it does not model

The injector **rewinds the consumer's own cursor**, in process, with franz-go's
`SetOffsets` — back to the offset of the record that faulted, carrying that
record's real leader epoch. That reproduces a crash between apply and commit
without killing anything: the effects ran, the offset never moved, the records
come back.

The obvious alternative does nothing at all. *Skipping the commit and carrying
on produces no duplicate*, silently: franz-go keeps the consume cursor in memory
and advances it on every poll, and reads the committed offset back only when a
partition is assigned. An uncommitted batch is simply never seen again. A harness
built that way reports zero duplicates and looks like it works.

What this **does** model: one process, crashing between apply and commit.

What it **does not** model, and these are different failures with different
answers:

- a **consumer group rebalance** moving a partition to another member,
- a **restart**, where the in-memory store starts empty,
- a **second consumer instance**, which has its own store and shares nothing.

### A rewind redelivers a tail — size for that, not for the fault rate

Sizing on **"1% of commits lost ≈ 1% duplicates" is wrong by more than an order
of magnitude.** On the graded run below, 56 injected faults produced 1948
double applies — **35 duplicate applications per fault**. On a shorter sizing
run beforehand the same injector gave **4.5×**.

It is a range rather than a constant because **the multiplier is broker batch
composition.** A cursor is per *partition*: rewinding to one record replays that
record **and every later record of that partition the loop had already polled**.
As lag builds the consumer polls larger batches, so each rewind replays more —
which is the whole distance between those two figures. Nothing about the
injector changed between them.

That is not an artefact of the harness. It is what a crash does: the process
does not lose one message, it loses everything applied since the last commit.

### Measured

Both arms: 3 partitions, fault rate `0.01`, seed `pf-s313`, a pinned run nonce so
each arm is fed byte-identical record keys, a 50 000-key store with a 10-minute
window, and `./stop.sh` between them so the second arm does not inherit the
first's offsets. Raw captures — metric scrapes, full consumer logs, image
digests — are committed under `results/pf-s313/capture/`.

| | at-least-once (`KL_DEDUPE=false`) | idempotent (`KL_DEDUPE=true`) |
|---|---|---|
| records the loop finished | 6773 | 7515 |
| effects run | 6773 | 5389 |
| **applied a second time** | **1948** | **0** |
| duplicates suppressed | 0 | 2126 |
| keys faulted | 56 | 59 |
| cursor rewinds | 27 | 36 |
| redeliveries in the log | 1948 | 2126 |
| store evictions | 0 | 0 |
| store expiries | 0 | 0 |

**The at-least-once arm applied 1948 records a second time**, from 56 injected
faults — the tail effect described above.

**The idempotent arm applied nothing twice.** It saw *more* redeliveries — 2126,
because it ran slightly longer — and suppressed every one of them.

Every figure is checked against the committed captures by
`internal/results`, which fails the build if this table and those files ever
disagree. The two loss counters being zero is what makes the rest readable: a
store that had been evicting would produce double applies of its own, and the
number would be measuring the store instead of the delivery semantics.

The fault sets are also identical. Both arms fired on the same keys over the
records both actually delivered — 54 keys across the 3 partitions — because the
decision is `sha256(seed ‖ key)` and not a draw from a random stream, so batch
boundaries and delivery order cannot move it.

### Scope, stated as properties rather than caveats

- **The store is in-process and bounded.** With capacity *C* and window *T*, a
  redelivery arriving later than *T* after the first sighting is **not caught**,
  and neither is one whose key was pushed out by more than *C* distinct keys
  arriving since. Defaults are C = 50 000 and T = 10 minutes, and the window
  matches the topic's retention on purpose: a record the broker has aged out
  cannot be redelivered, so remembering it longer buys nothing. Both losses are
  counted and published as `kafka_lab_dedupe_evictions_total` and
  `kafka_lab_dedupe_expiries_total`, labelled by store.
- **The guarantee does not survive a restart or a rebalance.** Nothing is written
  down. A new process, or a partition moving to another member, starts with an
  empty store, and every message still in flight is first-seen again.
- **Kafka transactions and exactly-once semantics are out of scope.** They are
  not implemented, not measured, and not approximated here. This lab demonstrates
  at-least-once delivery and an idempotent consumer, which is a different
  technique with a different cost.
- **A record with no identity header cannot be deduplicated.** It is applied and
  counted in `kafka_lab_records_without_dedupe_key_total` rather than dropped or
  quietly treated as unique. It was zero on both arms above.

## Notes and limits

- **One broker.** Every replication factor is 1. This lab is about backpressure
  and consumer lag, not about durability or failover, and a three-broker
  compose file would triple the startup time for nothing the demo shows.
- **One consumer.** Scaling the consumer group is an interesting demo and a
  different one; here the lag you build is drained by a single member, so the
  numbers stay easy to follow.
- **No throughput claims.** The rates you see are whatever the sliders ask for,
  bounded by one goroutine's simulated work delay on a laptop running eight
  containers. Nothing here is a benchmark of Kafka or of anything else.
- **`events` retains 10 minutes.** Long enough to build and drain a large
  backlog, short enough that a lab left running overnight does not fill a disk.

## License

MIT — see [LICENSE](LICENSE).
