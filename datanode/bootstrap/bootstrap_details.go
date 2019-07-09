package bootstrap

import (
	"encoding/json"
	"sync"
)

type status int

const (
	notApplicable status = iota
	copyNeeded
	copyFinished
)

type bootstrapDetails struct {
	*sync.RWMutex

	BootstrapStage BootstrapStage `json:"stage"`
	NumColumns     int            `json:"numColumns"`
	// map from batch id to slice of all columns
	Batches map[int32][]status `json:"batches"`
}

func NewBootstrapDetails() BootstrapDetails {
	return &bootstrapDetails{
		RWMutex:        &sync.RWMutex{},
		BootstrapStage: Waiting,
		Batches:        make(map[int32][]status),
	}
}

func (b *bootstrapDetails) SetNumColumns(numColumns int) {
	b.Lock()
	defer b.Unlock()
	b.NumColumns = numColumns
}

func (b *bootstrapDetails) AddVPToCopy(batchID int32, columnID uint32) {
	b.Lock()
	defer b.Unlock()

	if _, ok := b.Batches[batchID]; !ok {
		b.Batches[batchID] = make([]status, b.NumColumns)
	}

	if int(columnID) < b.NumColumns {
		b.Batches[batchID][columnID] = copyNeeded
	}
}

func (b *bootstrapDetails) SetBootstrapStage(stage BootstrapStage) {
	b.Lock()
	defer b.Unlock()
	b.BootstrapStage = stage
}

func (b *bootstrapDetails) MarkVPFinished(batchID int32, columnID uint32) {
	b.Lock()
	defer b.Unlock()

	if _, ok := b.Batches[batchID]; !ok {
		b.Batches[batchID] = make([]status, b.NumColumns)
	}

	if int(columnID) < b.NumColumns {
		b.Batches[batchID][columnID] = copyFinished
	}
}

func (b *bootstrapDetails) Clear() {
	b.Lock()
	defer b.Unlock()
	b.NumColumns = 0
	b.Batches = make(map[int32][]status)
	b.BootstrapStage = Waiting
}

func (b *bootstrapDetails) MarshalJSON() ([]byte, error) {
	type alias bootstrapDetails
	b.RLock()
	defer b.RUnlock()
	return json.Marshal((*alias)(b))
}
