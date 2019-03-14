package job

import (
	"time"

	"github.com/uber-go/tally"
	"github.com/uber/aresdb/client"
	"github.com/uber/aresdb/subscriber/common/database"
	"github.com/uber/aresdb/subscriber/common/rules"
	"github.com/uber/aresdb/subscriber/config"
)

const defaultInitInterval = 5 * time.Second
const defaultMultiplier = float32(1.5)
const defaultMaxElapsedTime = 10 * time.Minute

// RetryFailureHandler implements
// exponential backoff retry
type RetryFailureHandler struct {
	serviceConfig  config.ServiceConfig
	scope          tally.Scope
	database       database.Database
	jobName        string
	maxElapsedTime time.Duration
	elapsedTime    time.Duration
	multiplier     float32
	interval       time.Duration
}

// NewRetryFailureHandler creates a new RetryFailureHandler
func NewRetryFailureHandler(
	config rules.RetryFailureHandlerConfig,
	serviceConfig config.ServiceConfig,
	db database.Database,
	jobName string) *RetryFailureHandler {
	maxElapsedTime := defaultMaxElapsedTime
	if config.MaxRetryMinutes > 0 {
		maxElapsedTime = time.Duration(config.MaxRetryMinutes) * time.Minute
	}

	multiplier := defaultMultiplier
	// only support constant or increasing interval
	if config.Multiplier >= 1 {
		multiplier = config.Multiplier
	}

	initInterval := defaultInitInterval
	if config.InitRetryIntervalInSeconds > 0 {
		initInterval = time.Duration(config.InitRetryIntervalInSeconds) * time.Second
	}

	return &RetryFailureHandler{
		serviceConfig: serviceConfig,
		scope: serviceConfig.Scope.Tagged(map[string]string{
			"job":         jobName,
			"aresCluster": db.Cluster(),
		}),
		database:       db,
		jobName:        jobName,
		maxElapsedTime: maxElapsedTime,
		elapsedTime:    0,
		multiplier:     multiplier,
		interval:       initInterval,
	}
}

// HandleFailure handles failure with retry
func (handler *RetryFailureHandler) HandleFailure(destination database.Destination, rows []client.Row) (err error) {
	timer := time.NewTimer(0)
	for handler.elapsedTime+handler.interval < handler.maxElapsedTime {
		timer.Reset(handler.interval)
		select {
		case <-timer.C:
			err = handler.database.Save(destination, rows)
			if err == nil {
				timer.Stop()
				handler.elapsedTime = 0
				return nil
			}
			handler.elapsedTime += handler.interval
			handler.interval = time.Duration(float32(handler.interval) * handler.multiplier)
			handler.scope.Counter("message.retry.count").Inc(1)
			handler.scope.Gauge("message.retry.elapsedTime").Update(float64(handler.elapsedTime))
		}
	}
	timer.Stop()
	handler.elapsedTime = 0
	return err
}
