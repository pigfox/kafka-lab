#!/bin/sh
#
# run.sh — the only command you need.
#
# POSIX sh, not bash. This has to work from a bare clone on a stranger's laptop,
# and /bin/sh is the one interpreter that is always there — including on Alpine,
# on a minimal container, and on a BSD. Nothing below uses a bashism.
#
# ── EVERY CHECK EXITS NON-ZERO ──────────────────────────────────────────────
#
# A check that prints a warning and carries on is a log line, not a control. If
# something here detects a condition worth naming, it stops. The failure a
# reader most needs to hear about is the one that happened before the URLs were
# printed, and printing four URLs after a failed startup is how a broken lab
# gets reported as a working one.

set -eu

cd "$(dirname "$0")"

# THE PROJECT NAME IS PINNED, and it is pinned here rather than left to the
# `name:` field in docker-compose.yml because an ambient COMPOSE_PROJECT_NAME in
# the operator's shell OVERRIDES that field. This was not hypothetical: the
# machine this was built on exports COMPOSE_PROJECT_NAME=pf, so the stack came
# up under the project `pf` and shared a namespace with everything else that
# variable governs.
#
# The stakes are in stop.sh rather than here. `docker compose down
# --remove-orphans` removes every container in the PROJECT that the compose file
# does not declare — so under a hijacked project name it would reach out and
# delete containers belonging to somebody else's stack. Exporting the name makes
# that impossible regardless of what the surrounding shell says.
COMPOSE_PROJECT_NAME=kafka-lab
export COMPOSE_PROJECT_NAME

# ── configurable, with the same defaults docker-compose.yml carries ──────────
ADMIN_PORT="${KL_ADMIN_PORT:-18080}"
GRAFANA_PORT="${KL_GRAFANA_PORT:-18081}"
PROMETHEUS_PORT="${KL_PROMETHEUS_PORT:-18082}"
KAFKA_UI_PORT="${KL_KAFKA_UI_PORT:-18083}"
KAFKA_PORT="${KL_KAFKA_PORT:-19092}"

# How long to wait for admin to answer /healthz once compose reports the stack
# up. Bounded, because an unbounded wait is indistinguishable from a hang.
READY_TIMEOUT="${KL_READY_TIMEOUT:-120}"

say()  { printf '%s\n' "$*"; }
step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die()  { printf '\n\033[31merror: %s\033[0m\n' "$*" >&2; exit 1; }

# ── preflight ───────────────────────────────────────────────────────────────

step "Checking prerequisites"

if ! command -v docker >/dev/null 2>&1; then
	die "docker is not installed, or not on PATH.
       Install Docker Desktop or the Docker Engine, then run this again:
       https://docs.docker.com/get-docker/"
fi

if ! docker info >/dev/null 2>&1; then
	die "docker is installed but the daemon is not responding.
       Start Docker Desktop (or 'sudo systemctl start docker') and try again."
fi

# COMPOSE V2, specifically. `docker-compose` (the v1 Python script) does not
# support the `depends_on: condition: service_healthy` and
# `service_completed_successfully` conditions this stack relies on for its
# startup ordering, and it fails in a way that looks like a Kafka problem.
if ! docker compose version >/dev/null 2>&1; then
	die "Docker Compose v2 is required and was not found.
       You may have the older 'docker-compose' v1 script; this stack needs the
       'docker compose' subcommand, which ships with modern Docker.
       https://docs.docker.com/compose/install/"
fi

say "docker:         $(docker --version)"
say "docker compose: $(docker compose version --short 2>/dev/null || docker compose version)"

# ── port check ──────────────────────────────────────────────────────────────
#
# Compose will fail on a bound port anyway, but its error names the port and not
# what to do about it. Saying so up front, with the override, is the difference
# between a two-minute detour and a ten-minute one.

check_port() {
	port="$1"
	name="$2"
	var="$3"
	if command -v ss >/dev/null 2>&1; then
		if ss -ltn 2>/dev/null | grep -q "[:.]${port}[[:space:]]"; then
			die "port ${port} (${name}) is already in use.
       Free it, or pick another:  ${var}=$((port + 10000)) ./run.sh"
		fi
	fi
}

step "Checking ports"
check_port "$ADMIN_PORT"      "admin UI"   "KL_ADMIN_PORT"
check_port "$GRAFANA_PORT"    "Grafana"    "KL_GRAFANA_PORT"
check_port "$PROMETHEUS_PORT" "Prometheus" "KL_PROMETHEUS_PORT"
check_port "$KAFKA_UI_PORT"   "kafka-ui"   "KL_KAFKA_UI_PORT"
check_port "$KAFKA_PORT"      "Kafka"      "KL_KAFKA_PORT"
say "${ADMIN_PORT}, ${GRAFANA_PORT}, ${PROMETHEUS_PORT}, ${KAFKA_UI_PORT}, ${KAFKA_PORT} are free"

# ── start ───────────────────────────────────────────────────────────────────

step "Building and starting the stack"
say "First run builds four Go images and pulls four others; give it a few minutes."

# --wait blocks until every service with a healthcheck is healthy and every
# one-shot service has exited zero. Without it this script would race the stack
# it just started and poll /healthz against a container that does not exist yet.
if ! docker compose up -d --build --wait; then
	printf '\n\033[31mThe stack did not come up. Recent logs:\033[0m\n\n' >&2
	docker compose ps || true
	docker compose logs --tail=40 || true
	die "startup failed — see the logs above, then './stop.sh' before retrying"
fi

# ── readiness ───────────────────────────────────────────────────────────────
#
# compose --wait already waited on admin's healthcheck, which runs INSIDE the
# container. This polls from the HOST, which is a different claim: it proves the
# published port is actually forwarding. Those two have come apart before.

step "Waiting for the admin API on the host"

waited=0
until curl -fsS "http://localhost:${ADMIN_PORT}/healthz" >/dev/null 2>&1; do
	waited=$((waited + 2))
	if [ "$waited" -ge "$READY_TIMEOUT" ]; then
		printf '\n\033[31mAdmin did not answer within %ss. Recent logs:\033[0m\n\n' "$READY_TIMEOUT" >&2
		docker compose logs --tail=40 admin || true
		die "admin never became ready on http://localhost:${ADMIN_PORT}/healthz"
	fi
	sleep 2
done

say "admin answered after ${waited}s"

# ── done ────────────────────────────────────────────────────────────────────

cat <<BANNER

  kafka-lab is up.

    Control UI    http://localhost:${ADMIN_PORT}
    Grafana       http://localhost:${GRAFANA_PORT}
    Prometheus    http://localhost:${PROMETHEUS_PORT}
    kafka-ui      http://localhost:${KAFKA_UI_PORT}

    Kafka broker  localhost:${KAFKA_PORT}   (PLAINTEXT, from the host)

  What to do:

    1. Open the control UI. Producer and consumer both start at 50 msg/sec,
       so lag sits near zero.
    2. Drag the CONSUMER rate down to about 5. Watch the lag figure climb —
       the producer is filling the topic faster than the consumer drains it.
    3. Drag the consumer rate back up past the producer's. Watch lag fall to
       zero as the consumer works through the backlog.
    4. Open Grafana for the same story as one graph over time.

  Settings live on the compacted 'control' topic, so they survive a restart:
  'docker compose restart producer consumer' and they come back as you left them.

  Stop and reset:  ./stop.sh

BANNER
