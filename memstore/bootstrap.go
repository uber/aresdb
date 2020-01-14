//  Copyright (c) 2017-2018 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package memstore

import (
	"context"
	"io"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	m3Shard "github.com/m3db/m3/src/cluster/shard"
	xerrors "github.com/m3db/m3/src/x/errors"
	xretry "github.com/m3db/m3/src/x/retry"
	xsync "github.com/m3db/m3/src/x/sync"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/datanode/bootstrap"
	"github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	"github.com/uber/aresdb/utils"
)

// IsBootstrapped returns whether this table shard is bootstrapped.
func (shard *TableShard) IsBootstrapped() bool {
	shard.bootstrapLock.Lock()
	defer shard.bootstrapLock.Unlock()
	return shard.BootstrapState == bootstrap.Bootstrapped
}

// IsDiskDataAvailable returns whether the data is available on disk for table shard
func (shard *TableShard) IsDiskDataAvailable() bool {
	return atomic.LoadUint32(&shard.needPeerCopy) != 1
}

func (m *memStoreImpl) Bootstrap(
	peerSource client.PeerSource,
	origin string,
	topo topology.Topology,
	topoState *topology.StateSnapshot,
	options bootstrap.Options,
) error {
	// snapshot table shards not bootstrapped
	m.RLock()
	tableShards := make([]*TableShard, 0)
	for _, shardMap := range m.TableShards {
		for _, shard := range shardMap {
			if !shard.IsBootstrapped() {
				shard.Users.Add(1)
				tableShards = append(tableShards, shard)
			}
		}
	}
	m.RUnlock()

	// partition table shards based on whether it needs to copy data from peer
	// so that we can start process those doesn't first
	nonInitializingEnd := 0
	for i := 0; i < len(tableShards); i++ {
		// if doesn't need peer copy, swap into the first half
		if atomic.LoadUint32(&tableShards[i].needPeerCopy) == 0 {
			tableShards[i], tableShards[nonInitializingEnd] = tableShards[nonInitializingEnd], tableShards[i]
			nonInitializingEnd++
		}
	}

	workers := xsync.NewWorkerPool(options.MaxConcurrentTableShards())
	workers.Init()
	var (
		multiErr = xerrors.NewMultiError()
		mutex    sync.Mutex
		wg       sync.WaitGroup
	)

	for _, shard := range tableShards {
		shard := shard
		wg.Add(1)
		workers.Go(func() {
			err := shard.Bootstrap(peerSource, origin, topo, topoState, options)
			if err != nil {
				mutex.Lock()
				multiErr = multiErr.Add(err)
				mutex.Unlock()
			}
			wg.Done()
			shard.Users.Done()
		})
	}
	wg.Wait()
	return multiErr.FinalError()
}

// Bootstrap executes bootstrap for table shard
func (shard *TableShard) Bootstrap(
	peerSource client.PeerSource,
	origin string,
	topo topology.Topology,
	topoState *topology.StateSnapshot,
	options bootstrap.Options,
) error {
	shard.bootstrapLock.Lock()
	// check whether shard is already bootstrapping
	if shard.BootstrapState == bootstrap.Bootstrapping {
		shard.bootstrapLock.Unlock()
		return bootstrap.ErrTableShardIsBootstrapping
	}
	shard.BootstrapState = bootstrap.Bootstrapping
	shard.bootstrapLock.Unlock()

	shard.Schema.RLock()
	numColumns := len(shard.Schema.GetValueTypeByColumn())
	schema := shard.Schema.Schema
	shard.Schema.RUnlock()

	shard.BootstrapDetails.Clear()
	shard.BootstrapDetails.SetNumColumns(numColumns)
	success := false
	defer func() {
		shard.bootstrapLock.Lock()
		if success {
			shard.BootstrapState = bootstrap.Bootstrapped
		} else {
			shard.BootstrapState = bootstrap.BootstrapNotStarted
		}
		shard.bootstrapLock.Unlock()
	}()

	if atomic.LoadUint32(&shard.needPeerCopy) == 1 {
		shard.BootstrapDetails.SetBootstrapStage(bootstrap.PeerCopy)
		// find peer node for copy metadata and raw data
		peerNodes, err := shard.findBootstrapSource(origin, topo, topoState)
		if err != nil {
			utils.GetLogger().
				With("table", shard.Schema.Schema.Name).
				With("shardID", shard.ShardID).
				With("origin", origin).
				With("error", err.Error()).
				Error("failed to find peers for shard bootstrap")
			return err
		}

		// Note: skip peer copy step when we found no peers. this is correct based on the assumption that
		// if a shard replica does not find any peer in available/leaving state
		// then the cluster should be in the initial phase of new placement created
		// when we do replace/remove/add, the total number of shard replica remains the same
		// so the number of initializing replica should match the number of leaving replica
		// when we increase the number of replicas, we should always find existing available replica
		if len(peerNodes) == 0 {
			utils.GetLogger().
				With("table", shard.Schema.Schema.Name).
				With("shardID", shard.ShardID).
				With("origin", origin).
				Info("no available/leaving source, new placement")
		} else {
			utils.GetLogger().
				With("table", shard.Schema.Schema.Name).
				With("shardID", shard.ShardID).
				With("peers", peerNodes).
				Info("found peer to bootstrap from")

			// shuffle peer nodes randomly
			rand.New(rand.NewSource(utils.Now().Unix())).Shuffle(len(peerNodes), func(i, j int) {
				peerNodes[i], peerNodes[j] = peerNodes[j], peerNodes[i]
			})

			var dataStreamErr error
			borrowErr := peerSource.BorrowConnection(peerNodes, func(peerID string, nodeClient rpc.PeerDataNodeClient) {
				shard.BootstrapDetails.SetSource(peerID)
				dataStreamErr = shard.fetchDataFromPeer(peerID, nodeClient, origin, options)
			})

			if borrowErr != nil {
				return borrowErr
			}
			if dataStreamErr != nil {
				return dataStreamErr
			}
		}
		atomic.StoreUint32(&shard.needPeerCopy, 0)
	}

	// load metadata from disk
	err := shard.LoadMetaData()
	if err != nil {
		return err
	}

	shard.BootstrapDetails.SetBootstrapStage(bootstrap.Preload)
	// preload snapshot or archive batches into memory
	if schema.IsFactTable {
		// preload all columns for fact table
		endDay := int(utils.Now().Unix() / 86400)
		for columnID, column := range schema.Columns {
			if column.Deleted {
				continue
			}
			shard.PreloadColumn(columnID, endDay-column.Config.PreloadingDays, endDay)
		}
	} else {
		// preload snapshot for dimension table
		err = shard.LoadSnapshot()
		if err != nil {
			return err
		}
	}

	shard.BootstrapDetails.SetBootstrapStage(bootstrap.Recovery)
	// start play redolog
	shard.PlayRedoLog()
	success = true
	shard.BootstrapDetails.SetBootstrapStage(bootstrap.Finished)
	return nil
}

type vpRawDataRequest struct {
	tableShardMeta *rpc.TableShardMetaData
	batchMeta      *rpc.BatchMetaData
	vpMeta         *rpc.VectorPartyMetaData
}

// fetchDataFromPeer fetch metadata and raw vector party data from peer
func (shard *TableShard) fetchDataFromPeer(
	peerID string,
	client rpc.PeerDataNodeClient,
	origin string,
	options bootstrap.Options,
) error {

	sessionID, doneFn, err := shard.startStreamSession(peerID, client, origin, options)
	if err != nil {
		return err
	}
	defer doneFn()

	// 1. fetch meta data
	tableShardMeta, err := shard.fetchBatchMetaDataFromPeer(origin, sessionID, client)
	if err != nil {
		return err
	}

	// 2. set metadata and trigger recovery
	if err := shard.setTableShardMetadata(tableShardMeta); err != nil {
		return err
	}

	// 3. fetch raw vps
	workerPool := xsync.NewWorkerPool(options.MaxConcurrentStreamsPerTableShards())
	workerPool.Init()

	retrier := xretry.NewRetrier(xretry.NewOptions().SetMaxRetries(3))
	var (
		mutex  sync.Mutex
		errors xerrors.MultiError
		wg     sync.WaitGroup
	)

	for _, batchMeta := range tableShardMeta.Batches {
		err := shard.setBatchMetadata(tableShardMeta, batchMeta)
		if err != nil {
			return utils.StackError(err, "failed to set batch level metadata")
		}

		for _, vpMeta := range batchMeta.Vps {
			shard.BootstrapDetails.AddVPToCopy(batchMeta.GetBatchID(), vpMeta.GetColumnID())
		}
	}

	fetchStart := utils.Now()
	for _, batchMeta := range tableShardMeta.Batches {
		for _, vpMeta := range batchMeta.Vps {
			// capture batchMeta and vpMeta
			batchMeta := batchMeta
			vpMeta := vpMeta
			wg.Add(1)
			// TODO: add checksum to vp file and vpMeta to avoid copying existing data on disk
			workerPool.Go(func() {
				defer wg.Done()
				attempts := 0
				err = retrier.Attempt(func() error {
					attempts++
					request, vpWriter, err := shard.createVectorPartyRawDataRequest(origin, sessionID, tableShardMeta, batchMeta, vpMeta)
					if err != nil {
						utils.GetLogger().
							With("peer", peerID, "table", shard.Schema.Schema.Name, "shard", shard.ShardID, "batch", batchMeta.GetBatchID(), "column", vpMeta.GetColumnID(), "request", request, "error", err.Error()).
							Errorf("failed to create vector party raw data request, attempt %d", attempts)
						return err
					}
					defer vpWriter.Close()

					fetchStart := utils.Now()
					bytesFetched, err := shard.fetchVectorPartyRawDataFromPeer(client, vpWriter, request)
					if err != nil {
						utils.GetLogger().
							With("peer", peerID, "table", shard.Schema.Schema.Name, "shard", shard.ShardID, "batch", batchMeta.GetBatchID(), "column", vpMeta.GetColumnID(), "request", request, "error", err.Error()).
							Errorf("failed to fetch data from peer, attempt %d", attempts)
						return err
					}

					duration := utils.Now().Sub(fetchStart)
					utils.GetLogger().
						With("peer", peerID, "table", shard.Schema.Schema.Name, "shard", shard.ShardID, "batch", batchMeta.GetBatchID(), "column", vpMeta.GetColumnID(), "request", request).
						Infof("successfully fetched data (%d bytes) from peer, took %f seconds, attempt %d", bytesFetched, duration.Seconds(), attempts)

					utils.GetReporter(tableShardMeta.Table, int(tableShardMeta.Shard)).
						GetChildTimer(map[string]string{
							"batch":  strconv.Itoa(int(batchMeta.GetBatchID())),
							"column": strconv.Itoa(int(vpMeta.GetColumnID())),
						}, utils.RawVPFetchTime).Record(duration)

					utils.GetReporter(tableShardMeta.Table, int(tableShardMeta.Shard)).GetChildCounter(
						map[string]string{
							"batch":  strconv.Itoa(int(batchMeta.GetBatchID())),
							"column": strconv.Itoa(int(vpMeta.GetColumnID())),
						}, utils.RawVPBytesFetched).Inc(int64(bytesFetched))

					utils.GetReporter(tableShardMeta.Table, int(tableShardMeta.Shard)).
						GetChildGauge(map[string]string{
							"batch":  strconv.Itoa(int(batchMeta.GetBatchID())),
							"column": strconv.Itoa(int(vpMeta.GetColumnID())),
						}, utils.RawVPFetchBytesPerSec).Update(float64(bytesFetched) / duration.Seconds())

					shard.BootstrapDetails.MarkVPFinished(batchMeta.GetBatchID(), vpMeta.GetColumnID())
					return nil
				})

				if err != nil {
					mutex.Lock()
					errors = errors.Add(err)
					mutex.Unlock()
					utils.GetReporter(tableShardMeta.GetTable(), int(tableShardMeta.GetShard())).GetCounter(utils.RawVPFetchFailure).Inc(1)
				} else {
					utils.GetReporter(tableShardMeta.GetTable(), int(tableShardMeta.GetShard())).GetCounter(utils.RawVPFetchSuccess).Inc(1)
				}
			})
		}
	}
	wg.Wait()
	if !errors.Empty() {
		return errors.FinalError()
	}
	utils.GetReporter(tableShardMeta.Table, int(tableShardMeta.Shard)).GetTimer(utils.TotalRawVPFetchTime).Record(utils.Now().Sub(fetchStart))
	return nil
}

func (shard *TableShard) fetchBatchMetaDataFromPeer(origin string, sessionID int64, client rpc.PeerDataNodeClient) (*rpc.TableShardMetaData, error) {
	var (
		endBatchID   int32 = math.MaxInt32
		startBatchID int32 = math.MinInt32
	)

	shard.Schema.RLock()
	if shard.Schema.Schema.IsFactTable {
		endBatchID = int32(utils.Now().Unix() / 86400)
	}
	if shard.Schema.Schema.IsFactTable && shard.Schema.Schema.Config.RecordRetentionInDays > 0 {
		startBatchID = endBatchID - int32(shard.Schema.Schema.Config.RecordRetentionInDays) + 1
	}
	shard.Schema.RUnlock()

	req := &rpc.TableShardMetaDataRequest{
		Table:        shard.Schema.Schema.Name,
		Incarnation:  int32(shard.Schema.Schema.Incarnation),
		Shard:        uint32(shard.ShardID),
		StartBatchID: startBatchID,
		SessionID:    sessionID,
		NodeID:       origin,
		EndBatchID:   endBatchID,
	}
	return client.FetchTableShardMetaData(context.Background(), req)
}

func (shard *TableShard) createVectorPartyRawDataRequest(
	origin string,
	sessionID int64,
	tableMeta *rpc.TableShardMetaData,
	batchMeta *rpc.BatchMetaData,
	vpMeta *rpc.VectorPartyMetaData,
) (rawVPDataRequest *rpc.VectorPartyRawDataRequest, vpWriter utils.WriteSyncCloser, err error) {
	if shard.Schema.Schema.IsFactTable {
		// fact table archive vp writer
		vpWriter, err = shard.diskStore.OpenVectorPartyFileForWrite(tableMeta.GetTable(),
			int(vpMeta.GetColumnID()),
			int(tableMeta.GetShard()), int(batchMeta.GetBatchID()),
			batchMeta.GetArchiveVersion().GetArchiveVersion(),
			batchMeta.GetArchiveVersion().GetBackfillSeq())

		rawVPDataRequest = &rpc.VectorPartyRawDataRequest{
			SessionID:   sessionID,
			NodeID:      origin,
			Table:       tableMeta.GetTable(),
			Shard:       tableMeta.GetShard(),
			Incarnation: tableMeta.GetIncarnation(),
			BatchID:     int32(batchMeta.GetBatchID()),
			Version: &rpc.VectorPartyRawDataRequest_ArchiveVersion{
				ArchiveVersion: &rpc.ArchiveVersion{
					ArchiveVersion: batchMeta.GetArchiveVersion().GetArchiveVersion(),
					BackfillSeq:    batchMeta.GetArchiveVersion().GetBackfillSeq(),
				},
			},
			ColumnID: vpMeta.GetColumnID(),
		}
	} else {
		// dimension table snapshot vp writer
		vpWriter, err = shard.diskStore.OpenSnapshotVectorPartyFileForWrite(
			tableMeta.GetTable(),
			int(tableMeta.GetShard()),
			tableMeta.GetDimensionMeta().GetSnapshotVersion().GetRedoFileID(),
			tableMeta.GetDimensionMeta().GetSnapshotVersion().GetRedoFileOffset(),
			int(batchMeta.GetBatchID()),
			int(vpMeta.GetColumnID()))

		rawVPDataRequest = &rpc.VectorPartyRawDataRequest{
			SessionID:   sessionID,
			NodeID:      origin,
			Table:       tableMeta.GetTable(),
			Shard:       tableMeta.GetShard(),
			Incarnation: tableMeta.GetIncarnation(),
			BatchID:     int32(batchMeta.GetBatchID()),
			Version: &rpc.VectorPartyRawDataRequest_SnapshotVersion{
				SnapshotVersion: tableMeta.GetDimensionMeta().GetSnapshotVersion(),
			},
			ColumnID: vpMeta.GetColumnID(),
		}
	}
	return
}

func (shard *TableShard) fetchVectorPartyRawDataFromPeer(
	client rpc.PeerDataNodeClient,
	vpWriter utils.WriteSyncCloser,
	request *rpc.VectorPartyRawDataRequest,
) (int, error) {
	stream, err := client.FetchVectorPartyRawData(context.Background(), request)
	if err != nil {
		return 0, err
	}

	totalBytes := 0
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalBytes, err
		}
		bytesWritten, err := vpWriter.Write(data.Chunk)
		if err != nil {
			return totalBytes, err
		}
		totalBytes += bytesWritten
	}

	if err = vpWriter.Sync(); err != nil {
		return totalBytes, utils.StackError(err, "failed to sync to disk")
	}
	return totalBytes, nil
}

func (shard *TableShard) setBatchMetadata(tableShardMeta *rpc.TableShardMetaData, batchMeta *rpc.BatchMetaData) error {
	if shard.Schema.Schema.IsFactTable {
		err := shard.metaStore.OverwriteArchiveBatchVersion(shard.Schema.Schema.Name, shard.ShardID,
			int(batchMeta.BatchID),
			batchMeta.GetArchiveVersion().GetArchiveVersion(),
			batchMeta.GetArchiveVersion().GetBackfillSeq(),
			int(batchMeta.GetSize()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (shard *TableShard) setTableShardMetadata(tableShardMeta *rpc.TableShardMetaData) error {
	// update kafka offsets
	err := shard.metaStore.UpdateRedoLogCommitOffset(shard.Schema.Schema.Name, shard.ShardID, tableShardMeta.GetKafkaOffset().GetCommitOffset())
	if err != nil {
		return utils.StackError(err, "failed to update kafka commit offset")
	}

	err = shard.metaStore.UpdateRedoLogCheckpointOffset(shard.Schema.Schema.Name, shard.ShardID, tableShardMeta.GetKafkaOffset().GetCheckPointOffset())
	if err != nil {
		return utils.StackError(err, "failed to update archiving cutoff")
	}

	if shard.Schema.Schema.IsFactTable {
		// update archiving low water mark cutoff and backfill progress for fact table
		err := shard.metaStore.UpdateArchivingCutoff(shard.Schema.Schema.Name, shard.ShardID, tableShardMeta.GetFactMeta().GetHighWatermark())
		if err != nil {
			return utils.StackError(err, "failed to update archiving cutoff")
		}

		err = shard.metaStore.UpdateBackfillProgress(shard.Schema.Schema.Name, shard.ShardID, tableShardMeta.GetFactMeta().GetBackfillCheckpoint().GetRedoFileID(), tableShardMeta.GetFactMeta().GetBackfillCheckpoint().GetRedoFileOffset())
		if err != nil {
			return utils.StackError(err, "failed to update backfill progress")
		}
	} else {
		// update snapshot pregress for dimension table
		err := shard.metaStore.UpdateSnapshotProgress(
			shard.Schema.Schema.Name,
			shard.ShardID, tableShardMeta.GetDimensionMeta().GetSnapshotVersion().GetRedoFileID(),
			tableShardMeta.GetDimensionMeta().GetSnapshotVersion().GetRedoFileOffset(),
			tableShardMeta.GetDimensionMeta().GetLastBatchID(),
			uint32(tableShardMeta.GetDimensionMeta().GetLastBatchSize()))
		if err != nil {
			return utils.StackError(err, "failed to update archiving cutoff")
		}
	}
	return nil
}

func (shard *TableShard) startStreamSession(peerID string, client rpc.PeerDataNodeClient, origin string, options bootstrap.Options) (sessionID int64, doneFn func(), err error) {
	done := make(chan struct{})
	ttl := int64(options.BootstrapSessionTTL())
	startSessionRequest := &rpc.StartSessionRequest{
		Table:  shard.Schema.Schema.Name,
		Shard:  uint32(shard.ShardID),
		NodeID: origin,
		Ttl:    ttl,
	}

	session, err := client.StartSession(context.Background(), startSessionRequest)
	if err != nil {
		return 0, nil, utils.StackError(err, "failed to start session")
	}
	sessionID = session.ID

	stream, err := client.KeepAlive(context.Background())
	if err != nil {
		return 0, nil, utils.StackError(err, "failed to create keep alive stream")
	}

	// send first keep alive request
	if err = xretry.NewRetrier(xretry.NewOptions()).Attempt(func() error {
		return stream.Send(&rpc.Session{ID: sessionID, NodeID: origin})
	}); err != nil {
		return 0, nil, utils.StackError(err, "failed to send keep alive session")
	}

	// send loop
	go func(stream rpc.PeerDataNode_KeepAliveClient) {
		for {
			ticker := time.NewTicker(time.Duration(atomic.LoadInt64(&ttl) / 2))
			select {
			case <-ticker.C:
				err = xretry.NewRetrier(xretry.NewOptions()).Attempt(func() error {
					return stream.Send(&rpc.Session{ID: sessionID, NodeID: origin})
				})
				if err != nil {
					utils.GetLogger().
						With(
							"table", shard.Schema.Schema.Name,
							"shard", shard.ShardID,
							"error", err.Error(),
							"peer", peerID).
						Error("failed to send keep alive session")
				}
			case <-done:
				err = stream.CloseSend()
				if err != nil {
					utils.GetLogger().
						With(
							"table",
							shard.Schema.Schema.Name,
							"shard", shard.ShardID,
							"error", err.Error(),
							"peer", peerID).
						Error("failed to close keep alive session")
				}
				return
			}
		}
	}(stream)

	// receive loop
	go func(stream rpc.PeerDataNode_KeepAliveClient) {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				// server closed the stream
				utils.GetLogger().With("table", shard.Schema.Schema.Name, "shard", shard.ShardID).Error("server closed keep alive session")
				return
			} else if err != nil {
				utils.GetLogger().With("table", shard.Schema.Schema.Name, "shard", shard.ShardID, "error", err.Error()).Error("received error from keep alive session")
				return
			}
			if resp.Ttl > 0 {
				atomic.StoreInt64(&ttl, resp.Ttl)
			}
		}
	}(stream)

	return sessionID, func() {
		close(done)
	}, nil
}

func (shard *TableShard) findBootstrapSource(
	origin string, topo topology.Topology, topoState *topology.StateSnapshot) ([]string, error) {

	hostShardStates, ok := topoState.ShardStates[topology.ShardID(shard.ShardID)]
	if !ok {
		// This shard was not part of the topology when the bootstrapping
		// process began.
		return nil, utils.StackError(nil, "shard does not exist in topology")
	}

	peers := make([]string, 0, topo.Get().HostsLen())
	for _, hostShardState := range hostShardStates {
		if hostShardState.Host.ID() == origin {
			// Don't take self into account
			continue
		}
		shardState := hostShardState.ShardState
		switch shardState {
		// Don't want to peer bootstrap from a node that has not yet completely
		// taken ownership of the shard.
		case m3Shard.Initializing:
			// Success cases - We can bootstrap from this host, which is enough to
			// mark this shard as bootstrappable.
		case m3Shard.Leaving:
			fallthrough
		case m3Shard.Available:
			peers = append(peers, hostShardState.Host.ID())
		case m3Shard.Unknown:
			fallthrough
		default:
		}
	}

	return peers, nil
}
