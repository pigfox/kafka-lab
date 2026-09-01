#!/bin/sh
#
# stop.sh — stop the lab and reset it to nothing.
#
# `down -v` removes the anonymous volumes along with the containers. The compose
# file declares no NAMED volumes precisely so that this is a genuine reset: a
# lab whose "fresh start" carries yesterday's consumer offsets and yesterday's
# settings record is a lab nobody can reason about from a cold run.
#
# If you want to keep state across a stop, use `docker compose stop` instead —
# that halts the containers without discarding their storage, and the settings
# record on the compacted control topic comes back with them.

set -eu

cd "$(dirname "$0")"

# PINNED, AND HERE IT IS LOAD-BEARING RATHER THAN TIDY. An ambient
# COMPOSE_PROJECT_NAME in the operator's shell overrides the `name:` field in
# docker-compose.yml, and `down --remove-orphans` removes every container in the
# project that the compose file does not declare. Under a hijacked project name
# that means deleting containers belonging to an unrelated stack. Exporting the
# name confines this script to the lab it is named after.
COMPOSE_PROJECT_NAME=kafka-lab
export COMPOSE_PROJECT_NAME

if ! docker compose version >/dev/null 2>&1; then
	printf '\033[31merror: Docker Compose v2 is not available; nothing to stop.\033[0m\n' >&2
	exit 1
fi

printf '\033[1m==> Stopping kafka-lab and removing its volumes\033[0m\n'
docker compose down -v --remove-orphans

printf '\nStopped. Nothing of the lab remains; ./run.sh starts it clean.\n'
