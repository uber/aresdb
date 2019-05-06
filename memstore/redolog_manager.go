package memstore

// RedologManager defines rodelog manager interface
type RedologManager interface {
	// WriteUpsertBatch writes upsert batch to the redolog manager
	WriteUpsertBatch(upsertBatch *UpsertBatch) (int64, uint32)
	// UpdateMaxEventTime update max eventime of given redolog file
	UpdateMaxEventTime(eventTime uint32, redoFile int64)
	// NextUpsertBatch returns a function yielding the next upsert batch
	NextUpsertBatch() func() (*UpsertBatch, int64, uint32)
	// CheckpointRedolog checkpoint event time cutoff (from archiving) and redologFileID and batchOffset (from backfill)
	// to redolog manager
	CheckpointRedolog(cutoff uint32, redoFileCheckpointed int64, batchOffset uint32) error
	// Close free resources held by redolog manager
	Close()
}

// KafkaRedologManager is interface for kafka based redolog manager
type KafkaRedologManager interface {
	RedologManager
	// ConsumeFrom start the consumer for kafka redolog manager from given kafka offset
	ConsumeFrom(offset int64) error
}
