package main

import (
	"os"
	"testing"

	"github.com/pigfox/kafka-lab/internal/config"
)

// topic-init is pure wiring: read config, dial, create, exit. What is worth
// asserting is that the DEFAULTS it wires are the ones docker-compose.yml
// expects, because the two are the same fact written in two files and only one
// of them is compiled.
func TestDefaultsMatchTheComposeExpectations(t *testing.T) {
	os.Unsetenv("KAFKA_BROKERS")
	os.Unsetenv("KL_EVENT_PARTITIONS")

	if got := config.Brokers("KAFKA_BROKERS", "kafka:9092"); len(got) != 1 || got[0] != "kafka:9092" {
		t.Fatalf("broker default: %v", got)
	}
	// More than one partition, or the demo cannot show a group spreading work,
	// and kafka-ui's partition column would be a single row forever.
	if got := config.Int("KL_EVENT_PARTITIONS", 3); got < 2 {
		t.Fatalf("partition default %d is too low to show a partitioned topic", got)
	}
}

func TestBrokersAreOverridableByEnvironment(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "a:1,b:2")
	got := config.Brokers("KAFKA_BROKERS", "kafka:9092")
	if len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Fatalf("got %v", got)
	}
}
