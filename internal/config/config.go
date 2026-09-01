// Package config reads process configuration from the environment.
//
// THERE IS NO .env FILE ANYWHERE IN THIS REPOSITORY, and that is deliberate
// rather than an omission. Every value below carries a literal default, so a
// bare clone runs with an empty environment; docker-compose.yml restates the
// same defaults with ${VAR:-default} syntax so an operator can override any of
// them by exporting a shell variable before ./run.sh. Two places state each
// default, which is a duplication accepted on purpose: the compose file has to
// show the knob to be discoverable, and the binary has to work when run
// outside compose.
//
// Nothing here reads a path. A lab that reaches outside its own clone is a lab
// that only runs on the machine it was written on.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns the environment value for key, or def when it is unset or
// empty. Empty is treated as unset because compose interpolation of an unset
// variable yields an empty string, not an absent one.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

// Int returns the environment value for key parsed as an int. An unparseable
// value falls back to def rather than failing the process: a demo that refuses
// to boot over a typo in an optional knob is worse than one that logs the
// default it used.
func Int(key string, def int) int {
	raw := String(key, "")
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

// Float returns the environment value for key parsed as a float64, or def.
func Float(key string, def float64) float64 {
	raw := String(key, "")
	if raw == "" {
		return def
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return def
	}
	return f
}

// Duration returns the environment value for key parsed by time.ParseDuration,
// or def.
func Duration(key string, def time.Duration) time.Duration {
	raw := String(key, "")
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return d
}

// Bool returns the environment value for key parsed by strconv.ParseBool, or def.
func Bool(key string, def bool) bool {
	raw := String(key, "")
	if raw == "" {
		return def
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return b
}

// Brokers splits a comma-separated broker list, dropping empty fields.
func Brokers(key, def string) []string {
	raw := String(key, def)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
