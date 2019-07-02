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
	return shard.bootstrapState == bootstrap.Bootstrapped
}

// IsDiskDataAvailable returns whether the data is available on disk for table shard
func (shard *TableShard) IsDiskDataAvailable() bool {
	return atomic.LoadUint32(&shard.needPeerCopy) != 1
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
	if shard.bootstrapState == bootstrap.Bootstrapping {
		shard.bootstrapLock.Unlock()
		return bootstrap.ErrTableShardIsBootstrapping
	}
	shard.bootstrapState = bootstrap.Bootstrapping
	shard.bootstrapLock.Unlock()

	success := false
	defer func() {
		shard.bootstrapLock.Lock()
		if success {
			shard.bootstrapState = bootstrap.Bootstrapped
		} else {
			shard.bootstrapState = bootstrap.BootstrapNotStarted
		}
		shard.bootstrapLock.Unlock()
	}()

	if atomic.LoadUint32(&shard.needPeerCopy) == 1 {
		// find peer node for copy metadata and raw data
		peerNode := shard.findBootstrapSource(origin, topo, topoState)
		if peerNode == nil {
			return utils.StackError(nil, "no peer node available")
		}

		var dataStreamErr error
		borrowErr := peerSource.BorrowConnection(peerNode.ID(), func(nodeClient rpc.PeerDataNodeClient) {
			dataStreamErr = shard.fetchDataFromPeer(peerNode, nodeClient, options)
		})

		if borrowErr != nil {
			return borrowErr
		}
		if dataStreamErr != nil {
			return dataStreamErr
		}
		atomic.StoreUint32(&shard.needPeerCopy, 0)
	}

	// load metadata from disk
	err := shard.LoadMetaData()
	if err != nil {
		return err
	}

	// preload snapshot or archive batches into memory
	shard.Schema.RLock()
	schema := shard.Schema.Schema
	shard.Schema.RUnlock()
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

	// start play redolog
	shard.PlayRedoLog()
	success = true
	return nil
}

type vpRawDataRequest struct {
	tableShardMeta *rpc.TableShardMetaData
	batchMeta      *rpc.BatchMetaData
	vpMeta         *rpc.VectorPartyMetaData
}

// fetchDataFromPeer fetch metadata and raw vector party data from peer
func (shard *TableShard) fetchDataFromPeer(
	peerHost topology.Host,
	client rpc.PeerDataNodeClient,
	options bootstrap.Options,
) error {

	doneFn, err := shard.startStreamSession(peerHost, client, options)
	if err != nil {
		return err
	}
	defer doneFn()

	// 1. fetch meta data
	tableShardMeta, err := shard.fetchBatchMetaDataFromPeer(client)
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

	var (
		mutex           sync.Mutex
		retryVPRequests []vpRawDataRequest
		errors          xerrors.MultiError
		wg              sync.WaitGroup
	)

	for _, batchMeta := range tableShardMeta.Batches {
		err := shard.setBatchMetadata(tableShardMeta, batchMeta)
		if err != nil {
			return utils.StackError(err, "failed to set batch level metadata")
		}

		for _, vpMeta := range batchMeta.Vps {
			// capture batchMeta and vpMeta
			batchMeta := batchMeta
			vpMeta := vpMeta
			wg.Add(1)
			// TODO: add checksum to vp file and vpMeta to avoid copying existing data on disk
			workerPool.Go(func() {
				defer wg.Done()
				request, vpWriter, err := shard.createVectorPartyRawDataRequest(tableShardMeta, batchMeta, vpMeta)
				if err != nil {
					mutex.Lock()
					retryVPRequests = append(retryVPRequests, vpRawDataRequest{tableShardMeta, batchMeta, vpMeta})
					errors = errors.Add(err)
					mutex.Unlock()
				}
				defer vpWriter.Close()

				bytesFetched, err := shard.fetchVectorPartyRawDataFromPeer(peerHost, client, vpWriter, request)
				if err != nil {
					utils.GetLogger().
						With("peer", peerHost.String(), "table", shard.Schema.Schema.Name, "shard", shard.ShardID, "batch", batchMeta.GetBatchID(), "column", vpMeta.GetColumnID(), "request", request, "error", err.Error()).
						Errorf("failed fetching data from peer")
					mutex.Lock()
					retryVPRequests = append(retryVPRequests, vpRawDataRequest{tableShardMeta, batchMeta, vpMeta})
					errors = errors.Add(err)
					mutex.Unlock()
				} else {
					utils.GetLogger().
						With("peer", peerHost.String(), "table", shard.Schema.Schema.Name, "shard", shard.ShardID, "batch", batchMeta.GetBatchID(), "column", vpMeta.GetColumnID(), "request", request).
						Infof("successfully fetched data (%d bytes) from peer", bytesFetched)
				}
			})
		}
	}
	wg.Wait()
	// TODO: add retry for failed vps
	// 4. retry for failed vector parties
	if !errors.Empty() {
		return errors.FinalError()
	}
	return nil
}

func (shard *TableShard) fetchBatchMetaDataFromPeer(client rpc.PeerDataNodeClient) (*rpc.TableShardMetaData, error) {
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
		EndBatchID:   endBatchID,
	}
	return client.FetchTableShardMetaData(context.Background(), req)
}

func (shard *TableShard) createVectorPartyRawDataRequest(
	tableMeta *rpc.TableShardMetaData,
	batchMeta *rpc.BatchMetaData,
	vpMeta *rpc.VectorPartyMetaData,
) (rawVPDataRequest *rpc.VectorPartyRawDataRequest, vpWriter io.WriteCloser, err error) {
	if shard.Schema.Schema.IsFactTable {
		// fact table archive vp writer
		vpWriter, err = shard.diskStore.OpenVectorPartyFileForWrite(tableMeta.GetTable(),
			int(vpMeta.GetColumnID()),
			int(tableMeta.GetShard()), int(batchMeta.GetBatchID()),
			batchMeta.GetArchiveVersion().GetArchiveVersion(),
			batchMeta.GetArchiveVersion().GetBackfillSeq())

		rawVPDataRequest = &rpc.VectorPartyRawDataRequest{
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
	peerHost topology.Host, client rpc.PeerDataNodeClient,
	vpWriter io.WriteCloser,
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

func (shard *TableShard) startStreamSession(peerHost topology.Host, client rpc.PeerDataNodeClient, options bootstrap.Options) (doneFn func(), err error) {
	done := make(chan struct{})
	ttl := int64(options.BootstrapSessionTTL())
	startSessionRequest := &rpc.StartSessionRequest{
		Table: shard.Schema.Schema.Name,
		Shard: uint32(shard.ShardID),
		Ttl:   ttl,
	}

	session, err := client.StartSession(context.Background(), startSessionRequest)
	if err != nil {
		return nil, utils.StackError(err, "failed to start session")
	}

	stream, err := client.KeepAlive(context.Background())
	if err != nil {
		return nil, utils.StackError(err, "failed to create keep alive stream")
	}

	// send loop
	go func(stream rpc.PeerDataNode_KeepAliveClient) {
		for {
			ticker := time.NewTicker(time.Duration(atomic.LoadInt64(&ttl) / 2))
			select {
			case <-ticker.C:
				err = xretry.NewRetrier(xretry.NewOptions()).Attempt(func() error {
					return stream.Send(session)
				})
				if err != nil {
					utils.GetLogger().
						With(
							"table", shard.Schema.Schema.Name,
							"shard", shard.ShardID,
							"error", err.Error(),
							"peer", peerHost.String()).
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
							"peer", peerHost.String()).
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
				utils.GetLogger().With("table", shard.Schema.Schema.Name, "shard", shard.ShardID).Error("received error from keep alive session")
				return
			}
			if resp.Ttl > 0 {
				atomic.StoreInt64(&ttl, resp.Ttl)
			}
		}
	}(stream)

	return func() {
		close(done)
	}, nil
}

func (shard *TableShard) findBootstrapSource(
	origin string, topo topology.Topology, topoState *topology.StateSnapshot) topology.Host {

	peers := make([]topology.Host, 0, topo.Get().HostsLen())
	hostShardStates, ok := topoState.ShardStates[topology.ShardID(shard.ShardID)]
	if !ok {
		// This shard was not part of the topology when the bootstrapping
		// process began.
		return nil
	}

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
			peers = append(peers, hostShardState.Host)
		case m3Shard.Unknown:
			fallthrough
		default:
		}
	}

	if len(peers) == 0 {
		utils.GetLogger().
			With("table", shard.Schema.Schema.Name).
			With("shardID", shard.ShardID).
			With("origin", origin).
			With("source", "").
			Info("no available bootstrap sorce")
		return nil
	}

	utils.GetLogger().
		With("table", shard.Schema.Schema.Name).
		With("shardID", shard.ShardID).
		Info("bootstrap peers")
	//TODO: add consideration on connection count for choosing peer candidate
	idx := rand.Intn(len(peers))
	return peers[idx]
}
