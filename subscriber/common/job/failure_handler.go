package job

import "github.com/uber/aresdb/subscriber/common/database"

const retryHandler string = "retry"

// FailureHandler interface will be implemented by failure handler
// that are used when saving to storage layer fails
type FailureHandler interface {

	// HandleFailure will provide a contingent plan to
	// keep track of failed save
	HandleFailure(destination database.Destination, rows []database.Row) error
}
