package bootstrap

import (
	"context"
	"errors"
	pb "github.com/uber/aresdb/datanode/generated/proto/rpc"
	"github.com/uber/aresdb/diskstore"
	"github.com/uber/aresdb/metastore/common"
	"github.com/uber/aresdb/utils"
	"io"
	"math/rand"
	"sync"
	"time"
)

const (
	chunkSize = 1024
)

var (
	errNoCallerID       = errors.New("caller node id not set in request")
	errNoSessionID      = errors.New("session id not set in request")
	errInvalidSessionID = errors.New("invalid session id")
	errInvalidRequset   = errors.New("invalid request, table/shard not match")
	errSessionExisting  = errors.New("The request table/shard already have session running from the same node")
)

type PeerDataNodeServerImpl struct {
	sync.RWMutex

	metaStore common.MetaStore
	diskStore diskstore.DiskStore

	// session id to sessionInfo map
	sessions map[int64]*sessionInfo
	// tracking of all sessions for each table/shard
	tableShardSessions map[tableShardPair][]int64
}

type tableShardPair struct {
	table   string
	shardID uint32
}

type sessionInfo struct {
	sessionID    int64
	table        string
	shardID      uint32
	nodeID       string
	addr         string
	lastLiveTime time.Time
	ttl          int64
}

func NewPeerDataNodeServer(metaStore common.MetaStore, diskStore diskstore.DiskStore) pb.PeerDataNodeServer {
	return &PeerDataNodeServerImpl{
		metaStore:          metaStore,
		diskStore:          diskStore,
		sessions:           make(map[int64]*sessionInfo),
		tableShardSessions: make(map[tableShardPair][]int64),
	}
}

// IsBootstrapRunning is to check if any bootstrap is running in the table/shard
func (p *PeerDataNodeServerImpl) IsBootstrapRunning(tableName string, shardID uint32) bool {
	p.RLock()
	defer p.RUnlock()

	_, ok := p.tableShardSessions[tableShardPair{table: tableName, shardID: shardID}]
	if !ok {
		return true
	}
	return false
}

// StartSession create new session for one table/shard/node, only One session can be established on one table/shard from one node
func (p *PeerDataNodeServerImpl) StartSession(ctx context.Context, req *pb.StartSessionRequest) (*pb.Session, error) {
	var err error
	sessionInfo := &sessionInfo{
		table:        req.Table,
		shardID:      req.Shard,
		nodeID:       req.NodeID,
		ttl:          req.Ttl,
		lastLiveTime: utils.Now(),
		sessionID:    rand.Int63(),
	}

	defer func() {
		if err == nil {
			logInfoMsg(sessionInfo, "started bootstrap session")
		} else {
			logErrorMsg(sessionInfo, err, "start bootstrap session failed")
		}
	}()

	if err = p.validateTable(req.Table, req.Shard); err != nil {
		return nil, err
	}

	if err = p.checkReqExist(sessionInfo); err != nil {
		return nil, err
	}

	p.addSession(sessionInfo)

	return &pb.Session{
		ID: sessionInfo.sessionID,
	}, nil
}

func (p *PeerDataNodeServerImpl) checkReqExist(s *sessionInfo) error {
	pair := tableShardPair{
		table:   s.table,
		shardID: s.shardID,
	}

	sessions, ok := p.tableShardSessions[pair]
	if !ok {
		return nil
	}
	for _, sid := range sessions {
		if p.sessions[sid].nodeID == s.nodeID {
			return errSessionExisting
		}
	}
	return nil
}

// KeepAlive is like client/server ping process, to notify health about each other
func (p *PeerDataNodeServerImpl) KeepAlive(stream pb.PeerDataNode_KeepAliveServer) error {
	utils.GetLogger().With("action", "bootstrap").Info("keep alive called")
	var sessionInfo *sessionInfo
	var err error

	defer func() {
		if sessionInfo != nil {
			if err == nil {
				logInfoMsg(sessionInfo, "keep alive stoped")
			} else {
				logErrorMsg(sessionInfo, err, "keep alive failed")
			}
		} else {
			utils.GetLogger().With("action", "bootstrap", "error", err).Error("keep alive stopped")
		}
	}()

	for {
		session, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if sessionInfo == nil {
			if err = p.validateSessionSource(session.ID, session.NodeID); err != nil {
				return err
			}
			sessionInfo, _ = p.getSession(session.ID)
		}
		// update last live time
		sessionInfo.lastLiveTime = utils.Now()

		if err := stream.Send(&pb.KeepAliveResponse{ID: session.ID, Ttl: sessionInfo.ttl}); err != nil {
			return err
		}
	}
	return nil
}

// FetchTableShardMetaData to retrieve all metadata for one table/shard
func (p *PeerDataNodeServerImpl) FetchTableShardMetaData(ctx context.Context, req *pb.TableShardMetaDataRequest) (*pb.TableShardMetaData, error) {
	sessionInfo := &sessionInfo{
		table:        req.Table,
		shardID:      req.Shard,
		nodeID:       req.NodeID,
	}
	var err error

	logInfoMsg(sessionInfo, "FetchTableShardMetaData called")
	defer func() {
		if err == nil {
			logInfoMsg(sessionInfo, "FetchTableShardMetaData succeed")
		} else {
			logErrorMsg(sessionInfo, err, "FetchTableShardMetaData failed")
		}
	}()

	if err = p.validateRequest(req.SessionID, req.NodeID, req.Table, req.Shard); err != nil {
		return nil, err
	}

	t, err := p.metaStore.GetTable(req.Table)
	if err != nil {
		return nil, err
	}

	commitOffset, err := p.metaStore.GetRedoLogCommitOffset(req.Table, int(req.Shard))
	if err != nil {
		return nil, err
	}
	checkpointOffset, err := p.metaStore.GetRedoLogCheckpointOffset(req.Table, int(req.Shard))
	if err != nil {
		return nil, err
	}

	m := &pb.TableShardMetaData{
		Table:       req.Table,
		Shard:       req.Shard,
		Incarnation: int32(t.Incarnation),
		KafkaOffset: &pb.KafkaOffset{
			CommitOffset:     commitOffset,
			CheckPointOffset: checkpointOffset,
		},
	}

	if !t.IsFactTable {
		// dimension table
		redoFileID, redoFileOffset, lastBatchID, lastBatchSize, err := p.metaStore.GetSnapshotProgress(req.Table, int(req.Shard))
		if err != nil {
			return nil, err
		}
		batchIDs, err := p.diskStore.ListSnapshotBatches(req.Table, int(req.Shard), redoFileID, redoFileOffset)
		if err != nil {
			return nil, err
		}

		batches := make([]*pb.BatchMetaData, len(batchIDs))

		for i, batchID := range batchIDs {
			columns, err := p.diskStore.ListSnapshotVectorPartyFiles(req.Table, int(req.Shard), redoFileID, redoFileOffset, batchID)
			if err != nil {
				return nil, err
			}
			vps := make([]*pb.VectorPartyMetaData, len(columns))
			for j, colID := range columns {
				vps[j] = &pb.VectorPartyMetaData{
					ColumnID: uint32(colID),
				}
			}
			batches[i] = &pb.BatchMetaData{
				BatchID: int32(batchID),
				Vps:     vps,
			}
		}
		m.Batches = batches
		m.Meta = &pb.TableShardMetaData_DimensionMeta{
			DimensionMeta: &pb.DimensionTableShardMetaData{
				LastBatchID:   lastBatchID,
				LastBatchSize: int32(lastBatchSize),
				SnapshotVersion: &pb.SnapshotVersion{
					RedoFileID:     redoFileID,
					RedoFileOffset: redoFileOffset,
				},
			},
		}
		return m, nil
	}

	// fact table
	cutoff, err := p.metaStore.GetArchivingCutoff(req.Table, int(req.Shard))
	if err != nil {
		return nil, err
	}
	redoFileID, redoFileOffset, err := p.metaStore.GetBackfillProgressInfo(req.Table, int(req.Shard))
	if err != nil {
		return nil, err
	}

	batchIDs, err := p.metaStore.GetArchiveBatches(req.Table, int(req.Shard), req.StartBatchID, req.EndBatchID)
	if err != nil {
		return nil, err
	}

	batches := make([]*pb.BatchMetaData, len(batchIDs))
	for i, batchID := range batchIDs {
		version, seq, size, err := p.metaStore.GetArchiveBatchVersion(req.Table, int(req.Shard), batchID, cutoff)
		if err != nil {
			return nil, err
		}
		columns, err := p.diskStore.ListArchiveBatchVectorPartyFiles(req.Table, int(req.Shard), batchID, version, seq)
		if err != nil {
			return nil, err
		}
		vps := make([]*pb.VectorPartyMetaData, len(columns))
		for j, colID := range columns {
			vps[j] = &pb.VectorPartyMetaData{
				ColumnID: uint32(colID),
			}
		}
		batches[i] = &pb.BatchMetaData{
			BatchID: int32(batchID),
			Size:    uint32(size),
			ArchiveVersion: &pb.ArchiveVersion{
				ArchiveVersion: version,
				BackfillSeq:    seq,
			},
			Vps: vps,
		}
	}

	m.Batches = batches
	m.Meta = &pb.TableShardMetaData_FactMeta{
		FactMeta: &pb.FactTableShardMetaData{
			HighWatermark: cutoff,
			BackfillCheckpoint: &pb.BackfillCheckpoint{
				RedoFileID:     redoFileID,
				RedoFileOffset: redoFileOffset,
			},
		},
	}

	return m, nil
}

func (p *PeerDataNodeServerImpl) FetchVectorPartyRawData(req *pb.VectorPartyRawDataRequest, stream pb.PeerDataNode_FetchVectorPartyRawDataServer) error {
	sessionInfo := &sessionInfo{
		table:        req.Table,
		shardID:      req.Shard,
		nodeID:       req.NodeID,
	}
	var err error

	var timeElapsed int64
	timeStart := utils.Now()

	logInfoMsg(sessionInfo, "FetchVectorPartyRawData called", "batch", req.BatchID, "col", req.ColumnID)
	defer func() {
		if err == nil {
			logInfoMsg(sessionInfo, "FetchVectorPartyRawData succeed", req.BatchID, "col", req.ColumnID, "timeused", timeElapsed)
		} else {
			logErrorMsg(sessionInfo, err, "FetchVectorPartyRawData failed", req.BatchID, "col", req.ColumnID)
		}
	}()

	if err = p.validateRequest(req.SessionID, req.NodeID, req.Table, req.Shard); err != nil {
		return err
	}

	t, err := p.metaStore.GetTable(req.Table)
	if err != nil {
		return err
	}

	var reader io.ReadCloser
	if t.IsFactTable {
		reader, err = p.diskStore.OpenSnapshotVectorPartyFileForRead(req.Table, int(req.Shard), int64(req.GetArchiveVersion().ArchiveVersion), req.GetArchiveVersion().BackfillSeq, int(req.BatchID), int(req.ColumnID))
	} else {
		reader, err = p.diskStore.OpenSnapshotVectorPartyFileForRead(req.Table, int(req.Shard), int64(req.GetSnapshotVersion().RedoFileID), req.GetSnapshotVersion().RedoFileOffset, int(req.BatchID), int(req.ColumnID))
	}
	if err != nil {
		return err
	}
	defer reader.Close()

	vp := &pb.VectorPartyRawData{}
	buf := make([]byte, chunkSize)
	done := false
	for !done {
		n, err := reader.Read(vp.Chunk)
		if err != nil {
			if err == io.EOF {
				done = true
			}
			return err
		}
		if n > 0 {
			vp.Chunk = buf[:n]
			if err = stream.Send(vp); err != nil {
				return err
			}
		}
	}
	// in milsec
	timeElapsed = utils.Now().Sub(timeStart).Nanoseconds() / 1000000

	return nil
}

func (p *PeerDataNodeServerImpl) validateSessionSource(sessionID int64, nodeID string) error {
	sessionInfo, err := p.getSession(sessionID)
	if err != nil {
		return err
	}
	if sessionInfo.nodeID != nodeID {
		return errInvalidRequset
	}
	return nil
}

func (p *PeerDataNodeServerImpl) validateRequest(sessionID int64, nodeID string, table string, shard uint32) error {
	sessionInfo, err := p.getSession(sessionID)
	if err != nil {
		return err
	}
	if sessionInfo.nodeID != nodeID {
		return errInvalidRequset
	}

	if sessionInfo.table != table || sessionInfo.shardID != shard {
		return errInvalidRequset
	}
	return nil
}

// record new requested session
func (p *PeerDataNodeServerImpl) addSession(session *sessionInfo) {
	p.Lock()
	defer p.Unlock()

	p.sessions[session.sessionID] = session
	tableShard := tableShardPair{
		table:   session.table,
		shardID: session.shardID,
	}
	if _, ok := p.tableShardSessions[tableShard]; !ok {
		p.tableShardSessions[tableShard] = []int64{}
	}
	p.tableShardSessions[tableShard] = append(p.tableShardSessions[tableShard], session.sessionID)
}

// retrieve session info using session id
func (p *PeerDataNodeServerImpl) getSession(sessionID int64) (*sessionInfo, error) {
	p.RLock()
	defer p.RUnlock()

	session, ok := p.sessions[sessionID]
	if !ok {
		return nil, errInvalidSessionID
	}
	return session, nil
}

// check if request table/shard is valid
func (p *PeerDataNodeServerImpl) validateTable(tableName string, shardID uint32) error {
	// check if table exists
	if _, err := p.metaStore.GetTable(tableName); err != nil {
		return err
	}
	//  TODO check table shard ownership from topology

	return nil
}

func logInfoMsg(s *sessionInfo, msg string, fields ...interface{}) {
	utils.GetLogger().With("action", "bootstrap", "table", s.table, "shard", s.shardID, fields).Info(msg)
}

func logErrorMsg(s *sessionInfo, err error, msg string, fields ...interface{}) {
	utils.GetLogger().With("action", "bootstrap", "table", s.table, "shard", s.shardID, "error", err, fields).Error(msg)
}
