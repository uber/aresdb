package datanode

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/uber/aresdb/cluster/topology"
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

type peer struct {
	sync.RWMutex
	sync.WaitGroup

	host topology.Host

	conn   *grpc.ClientConn
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

		p.conn, err = grpc.Dial(fmt.Sprintf("%s:%s", parsedURL.Hostname(), parsedURL.Port()), grpc.WithInsecure())
		if err != nil {
			p.Unlock()
			return err
		}
	}

	conn := p.conn
	p.Add(1)
	p.Unlock()

	fn(rpc.NewPeerDataNodeClient(conn))
	p.Done()
	return nil
}

// Close close the peer
func (p *peer) Close() {
	utils.GetLogger().With("host", p.host.String()).Info("closing peer connection")
	// waiting for on going operation with client connection
	p.Wait()

	p.Lock()
	defer p.Unlock()
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			utils.GetLogger().With("host", p.host.String(), "error", err.Error()).Error("failed to close grpc connection")
		}
		p.closed = true
		p.conn = nil
	}
}

// newPeer create a new peer object
func newPeer(host topology.Host) *peer {
	return &peer{
		host: host,
	}
}

type peerSource struct {
	sync.RWMutex

	watch topology.MapWatch
	peers map[string]*peer

	done chan struct{}
}

func (ps *peerSource) BorrowConnection(hostID string, fn client.WithConnectionFn) error {
	ps.RLock()
	peer, exist := ps.peers[hostID]
	if !exist {
		ps.RUnlock()
		return errPeerNotExist
	}
	ps.RUnlock()

	return peer.BorrowConnection(fn)
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
			ps.peers[host.ID()] = newPeer(host)
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
func NewPeerSource(topo topology.Topology) (client.PeerSource, error) {
	mapWatch, err := topo.Watch()
	if err != nil {
		return nil, err
	}

	<-mapWatch.C()
	topoMap := mapWatch.Get()

	ps := &peerSource{
		watch: mapWatch,
		done:  make(chan struct{}),
		peers: make(map[string]*peer),
	}

	ps.updateTopoMap(topoMap)
	go ps.watchTopoChange()

	return ps, nil
}
