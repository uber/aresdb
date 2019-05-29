package datanode

import (
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/utils"
	"sync"
	"time"
)

type Table struct {
	sync.RWMutex

	datanode       DataNode
	name           string
	bootstrapState BootstrapState
	metrics        tableMetrics
}

type tableMetrics struct {
	bootstrap      instrument.MethodMetrics
	unfulfilled    tally.Counter
	bootstrapStart tally.Counter
	bootstrapEnd   tally.Counter
}

func newTableMetrics(scope tally.Scope, samplingRate float64) tableMetrics {
	return tableMetrics{
		bootstrap:      instrument.NewMethodMetrics(scope, "bootstrap", samplingRate),
		unfulfilled:    scope.Counter("bootstrap.unfulfilled"),
		bootstrapStart: scope.Counter("bootstrap.start"),
		bootstrapEnd:   scope.Counter("bootstrap.end"),
	}
}

func NewTable(datanode DataNode, table string, opts Options) *Table {
	t := Table{
		name:     table,
		datanode: datanode,
		metrics:  newTableMetrics(opts.InstrumentOptions().MetricsScope(), opts.InstrumentOptions().MetricsSamplingRate()),
	}
	return &t
}

func (t *Table) Bootstrap(start time.Time, process bootstrap.Process) error {
	callStart := utils.Now()

	t.Lock()
	if t.bootstrapState == Bootstrapping {
		t.Unlock()
		t.metrics.bootstrap.ReportError(utils.Now().Sub(callStart))
		return errTableIsBootstrapping
	}
	t.bootstrapState = Bootstrapping
	t.Unlock()

	t.metrics.bootstrapStart.Inc(1)

	success := false
	defer func() {
		t.Lock()
		if success {
			t.bootstrapState = Bootstrapped
		} else {
			t.bootstrapState = BootstrapNotStarted
		}
		t.Unlock()
		t.metrics.bootstrapEnd.Inc(1)
	}()

	var (
		owned, err = t.datanode.MetaStore.GetOwnedShards(t.name)
		shards     = make([]databaseShard, 0, len(owned))
	)
	for _, shard := range owned {
		if !shard.IsBootstrapped() {
			shards = append(shards, shard)
		}
	}
	if len(shards) == 0 {
		success = true
		n.metrics.bootstrap.ReportSuccess(n.nowFn().Sub(callStart))
		return nil
	}

	shardIDs := make([]uint32, len(shards))
	for i, shard := range shards {
		shardIDs[i] = shard.ID()
	}

	bootstrapResult, err := process.Run(start, n.metadata, shardIDs)
	if err != nil {
		n.log.Error("bootstrap aborted due to error",
			zap.Stringer("namespace", n.id),
			zap.Error(err))
		return err
	}
	n.metrics.bootstrap.Success.Inc(1)

	// Bootstrap shards using at least half the CPUs available
	workers := xsync.NewWorkerPool(int(math.Ceil(float64(runtime.NumCPU()) / 2)))
	workers.Init()

	numSeries := bootstrapResult.DataResult.ShardResults().NumSeries()
	n.log.Info("bootstrap data fetched now initializing shards with series blocks",
		zap.Int("numShards", len(shards)),
		zap.Int64("numSeries", numSeries),
	)

	var (
		multiErr = xerrors.NewMultiError()
		results  = bootstrapResult.DataResult.ShardResults()
		mutex    sync.Mutex
		wg       sync.WaitGroup
	)
	for _, shard := range shards {
		shard := shard
		wg.Add(1)
		workers.Go(func() {
			var bootstrapped *result.Map
			if shardResult, ok := results[shard.ID()]; ok {
				bootstrapped = shardResult.AllSeries()
			} else {
				bootstrapped = result.NewMap(result.MapOptions{})
			}

			err := shard.Bootstrap(bootstrapped)

			mutex.Lock()
			multiErr = multiErr.Add(err)
			mutex.Unlock()

			wg.Done()
		})
	}

	wg.Wait()

	if n.reverseIndex != nil {
		err := n.reverseIndex.Bootstrap(bootstrapResult.IndexResult.IndexResults())
		multiErr = multiErr.Add(err)
	}

	markAnyUnfulfilled := func(label string, unfulfilled result.ShardTimeRanges) {
		shardsUnfulfilled := int64(len(unfulfilled))
		n.metrics.unfulfilled.Inc(shardsUnfulfilled)
		if shardsUnfulfilled > 0 {
			str := unfulfilled.SummaryString()
			err := fmt.Errorf("bootstrap completed with unfulfilled ranges: %s", str)
			multiErr = multiErr.Add(err)
			n.log.Error(err.Error(),
				zap.String("namespace", n.id.String()),
				zap.String("bootstrap-type", label),
			)
		}
	}
	markAnyUnfulfilled("data", bootstrapResult.DataResult.Unfulfilled())
	markAnyUnfulfilled("index", bootstrapResult.IndexResult.Unfulfilled())

	err = multiErr.FinalError()
	n.metrics.bootstrap.ReportSuccessOrError(err, n.nowFn().Sub(callStart))
	success = err == nil
	return err

}
