// Command topic-init creates the lab's topics and exits.
//
// IT IS A ONE-SHOT SERVICE RATHER THAN A LINE IN THE PRODUCER'S STARTUP, and
// that is worth the extra container. Three services need these topics; if each
// created them on boot, three racing CreateTopics calls would produce two
// harmless TOPIC_ALREADY_EXISTS errors and one success, and every startup log
// would carry two scary-looking errors that mean nothing. Worse, the topic
// CONFIGS — compaction on `control`, retention on `events` — would be set by
// whichever service happened to win, so a config change would need editing in
// three places to be reliable.
//
// It exits 0 when the topics exist, whether it created them or found them.
// compose's `depends_on: condition: service_completed_successfully` then gives
// every other service a topic guarantee rather than a timing hope.
package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/pigfox/kafka-lab/internal/config"
	"github.com/pigfox/kafka-lab/internal/kafkabus"
	"github.com/pigfox/kafka-lab/internal/runner"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := runner.SignalContext()
	defer stop()

	brokers := config.Brokers("KAFKA_BROKERS", "kafka:9092")
	partitions := config.Int("KL_EVENT_PARTITIONS", 3)
	dialEvery := config.Duration("KL_DIAL_RETRY", 2*time.Second)

	log.Info("waiting for the broker", "brokers", brokers)
	cl, err := kafkabus.DialRetry(ctx, log, brokers, dialEvery)
	if err != nil {
		log.Error("could not reach the broker", "error", err)
		os.Exit(1)
	}
	defer cl.Close()

	if err := kafkabus.EnsureTopics(ctx, cl, int32(partitions), log); err != nil {
		log.Error("could not create topics", "error", err)
		os.Exit(1)
	}

	log.Info("topics ready")
}
