package datanode

import (
	"context"
	"errors"
	"fmt"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/uber/aresdb/cluster/topology"
	"github.com/uber/aresdb/common"
	"github.com/uber/aresdb/datanode/client"
	"github.com/uber/aresdb/datanode/generated/proto/rpc"
	"github.com/uber/aresdb/utils"
	"google.golang.org/grpc"
	"net/url"
	"sync"
)

var (
	errPeerClosed   = errors.New("peer closed")
	errPeerNotExist = errors.New("peer does not exist")
)

var grpcDialer = func(target string, opts ...grpc.DialOption) (client rpc.PeerDataNodeClient, closeFn func() error, err error) {
	conn, err := grpc.Dial(fmt.Sprintf(target, grpc.WithInsecure()))
	if err != nil {
		return nil, nil, err
	}
	return rpc.NewPeerDataNodeClient(conn), func() error { return conn.Close() }, nil
}

type peer struct {
	sync.RWMutex
	sync.WaitGroup

	host topology.Host

	dialer  client.PeerConnDialer
	conn    rpc.PeerDataNodeClient
	closeFn func() error

	closed bool
}

func (p *peer) Host() topology.Host {
	return p.host
}

// BorrowConnection from peer
func (p *peer) BorrowConnection(fn client.WithConnectionFn) (err error) {
	p.Lock()
	if p.closed {
		p.Unlock()
		return errPeerClosed
	}

	if p.conn == nil {
		parsedURL, err := url.Parse(p.host.Address())
		if err != nil {
			p.Unlock()
			return err
		}

		p.conn, p.closeFn, err = p.dialer(fmt.Sprintf("%s:%s", parsedURL.Hostname(), parsedURL.Port()), grpc.WithInsecure())
		if err != nil {
			p.Unlock()
			return err
		}
	}

	conn := p.conn
	p.Add(1)
	p.Unlock()

	defer p.Done()

	err = p.checkHealth(conn)
	if err != nil {
		return err
	}

	fn(p.host.ID(), conn)
	return nil
}

func (p *peer) checkHealth(peerNodeClient rpc.PeerDataNodeClient) error {
	healthCheckResponse, err := peerNodeClient.Health(context.Background(), &rpc.HealthCheckRequest{Service: "PeerData"})
	if err != nil {
		return xerrors.Wrapf(err, "failed to check health of %s", p.host.ID())
	}
	status := healthCheckResponse.GetStatus()
	if status != rpc.HealthCheckResponse_SERVING {
		return fmt.Errorf("unhealthy peer id: %s, status: %s", p.host.ID(), status.String())
	}
	return nil
}

// Close close the peer
func (p *peer) Close() {
	utils.GetLogger().With("host", p.host.String()).Info("closing peer connection")
	// waiting for on going operation with client connection
	p.Wait()

	p.Lock()
	defer p.Unlock()
	if p.conn != nil && p.closeFn != nil {
		err := p.closeFn()
		if err != nil {
			utils.GetLogger().With("host", p.host.String(), "error", err.Error()).Error("failed to close grpc connection")
		}
		p.closed = true
		p.conn = nil
	}
}

// newPeer create a new peer object
func newPeer(host topology.Host, dialer client.PeerConnDialer) *peer {
	return &peer{
		host:   host,
		dialer: dialer,
	}
}

type peerSource struct {
	sync.RWMutex

	logger common.Logger
	watch  topology.MapWatch
	peers  map[string]*peer
	dialer client.PeerConnDialer

	done chan struct{}
}

func (ps *peerSource) borrowConnection(hostID string, fn client.WithConnectionFn) error {
	ps.RLock()
	peer, exist := ps.peers[hostID]
	if !exist {
		ps.RUnlock()
		return errPeerNotExist
	}
	ps.RUnlock()

	return peer.BorrowConnection(fn)
}

func (ps *peerSource) BorrowConnection(hostIDs []string, fn client.WithConnectionFn) error {
	multiError := xerrors.NewMultiError()
	for _, hostID := range hostIDs {
		err := ps.borrowConnection(hostID, fn)
		if err != nil {
			ps.logger.With("host", hostID, "error", err.Error()).Warn("not able to borrow connection")
			multiError = multiError.Add(err)
		} else {
			return nil
		}
	}
	return multiError.FinalError()
}

func (ps *peerSource) watchTopoChange() {
	for {
		select {
		case <-ps.watch.C():
			ps.updateTopoMap(ps.watch.Get())
		case <-ps.done:
			return
		}
	}
}

func (ps *peerSource) Close() {
	close(ps.done)

	ps.Lock()
	defer ps.Unlock()
	for _, peer := range ps.peers {
		peer.Close()
	}
}

func (ps *peerSource) updateTopoMap(topoMap topology.Map) {
	ps.Lock()
	defer ps.Unlock()

	// unknown host
	knownHosts := make(map[string]struct{})
	for _, host := range topoMap.Hosts() {
		knownHosts[host.ID()] = struct{}{}
		if _, exist := ps.peers[host.ID()]; !exist {
			ps.peers[host.ID()] = newPeer(host, ps.dialer)
		}
	}

	for hostID, peer := range ps.peers {
		if _, known := knownHosts[hostID]; !known {
			delete(ps.peers, hostID)
			go func() {
				peer.Close()
			}()
		}
	}
}

// NewPeerSource creates PeerSource
func NewPeerSource(logger common.Logger, topo topology.Topology, dialerOverride client.PeerConnDialer) (client.PeerSource, error) {
	dialer := grpcDialer
	if dialerOverride != nil {
		dialer = dialerOverride
	}

	mapWatch, err := topo.Watch()
	if err != nil {
		return nil, err
	}

	<-mapWatch.C()
	topoMap := mapWatch.Get()

	ps := &peerSource{
		logger: logger,
		watch:  mapWatch,
		dialer: dialer,
		done:   make(chan struct{}),
		peers:  make(map[string]*peer),
	}

	ps.updateTopoMap(topoMap)
	go ps.watchTopoChange()

	return ps, nil
}
