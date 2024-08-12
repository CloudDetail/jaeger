package spanstore

import (
	"database/sql"
	"time"

	"go.uber.org/zap"
)

// WorkerParams contains parameters that are shared between WriteWorkers
type WorkerParams struct {
	logger     *zap.Logger
	db         *sql.DB
	indexTable TableName
	spansTable TableName
	tenant     string
	encoding   Encoding
	delay      time.Duration
}
