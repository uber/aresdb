package memstore

// RedologManagerConfig represents config to initialize redolog manager
type RedologManagerConfig struct {
	Kafka struct {
		Enabled           bool                     `json:"enabled"`
		Address           []string                 `json:"address"`
		CommitFunc        func(offset int64) error `json:"-"`
	} `json:"kafka"`
	Namespace string
	Table     string
	Shard     int32
}

// RedologManager defines rodelog manager interface
type RedologManager interface {
	WriteUpsertBatch(upsertBatch *UpsertBatch) (int64, uint32)
	UpdateMaxEventTime(eventTime uint32, redoFile int64)
	NextUpsertBatch() func() (*UpsertBatch, int64, uint32)
	PurgeRedologFileAndData(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error
	Close()
}

// KafkaRedologManager is interface for kafka based redolog manager
type KafkaRedologManager interface {
	RedologManager
	StartConsumer(offset int64) error
}
