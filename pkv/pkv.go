package pkv

import (
	context "context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/kr/pretty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	ErrInvalidVersion  = errors.New("invalid version")
	ErrNotEnoughQuorum = errors.New("not enough quorum")
	ErrInvalidPointer  = errors.New("invalid pointer")
	ServerAddrs        = [...]string{"localhost:3001", "localhost:3002", "localhost:3003"}
	Quorum             = 2
)

type Version struct {
	lock sync.Mutex
	// These need to be pointers because we need to differentiate existing version
	// from new version. If the pointers are nil we know it's a new version.
	pLastRnd *ProposalNum
	pVal     *Value
	pVRnd    *ProposalNum
	// Save the version number of the accepted value.
	Version int64
}

func RndStr(p *ProposalNum) string {
	if p == nil {
		return "(nil)"
	} else {
		return fmt.Sprintf("(N:%d, ProposerId:%d)", p.N, p.ProposerId)
	}
}

func (v *Version) String() string {
	if v.pVal == nil {
		return fmt.Sprintf("<V=nil, VRnd=%s, LastRnd=%s, Ver=%d>",
			RndStr(v.pVRnd), RndStr(v.pLastRnd), v.Version)
	} else {
		return fmt.Sprintf("<V=%d, VRnd=%s, LastRnd=%s, Ver=%d>",
			v.pVal.V, RndStr(v.pVRnd), RndStr(v.pLastRnd), v.Version)
	}
}

func (req *PkvRequest) MyString() string {
	return fmt.Sprintf("<Type:%s, Id:(%s), Rnd:%s, Val:%s>", req.Type, req.Id, RndStr(req.Rnd), req.Val)
}

func (r *PkvResponse) MyString() string {
	return fmt.Sprintf("<Type:%s, LastRnd:%s, Val:%s, VRnd:%s>", r.Type, RndStr(r.LastRnd), r.Val, RndStr(r.VRnd))
}

type VersionedValue struct {
	V       int64
	Version int64
}

type Versions map[int64]*Version

type Snapshot struct {
	lock  sync.Mutex
	vvals map[string]*VersionedValue
}

type Acceptor struct {
	lock sync.Mutex

	// Core storage: map key -> series of versions.
	KV map[string]Versions

	// Remember the latest version of each key entry, so that proposer does not
	// have to know the version number. It can simply pass version -1 to indicate
	// the latest version.
	latest map[string]int64

	// Snapshot of the KV pairs.
	// Snapshot map[string]*VersionedValue
	snapshot Snapshot

	UnimplementedPkvServer
}

func (s *Acceptor) getLockedVersion(id *InstanceId) (*Version, error) {
	pretty.Logf("[Acceptor] getLockedVersion for instance: %v", id)
	s.lock.Lock()
	defer s.lock.Unlock()

	key := id.Key
	version := id.Version
	// Entry is a series of versions corresponding to the key.
	entry, found := s.KV[key]
	if !found {
		// Create a new entry for the key.
		pretty.Logf("[Acceptor] create new entry for key: %v", id.Key)
		entry = Versions{}
		s.KV[key] = entry
	}

	if version < 0 {
		// The proposer wants the latest version. In this case we'll create a new
		// version with version number being LastVersion + 1.
		// What if this is a new entry? In this case there is no latest version
		// stored in the map and we'll get latest = 0. This also means that versions
		// will start from 1.
		// NOTE that we are not updating the latest map here - this should be done
		// when a value is ACCEPTED.
		latest, _ := s.latest[key]
		version = latest + 1
		pretty.Logf("[Acceptor] latest version %d, new version %d", latest, version)
	}
	pVer, found := entry[version]
	if !found {
		// Create a new version for the entry.
		entry[version] = &Version{
			pLastRnd: &ProposalNum{},
			// pVal is nil: this a new version.
			pVal:    nil,
			pVRnd:   &ProposalNum{},
			Version: version,
		}
		pVer = entry[version]
	}

	pretty.Logf("[Acceptor] getLockedVersion return: %s", pVer)
	pVer.lock.Lock()

	return pVer, nil
}

func (p *ProposalNum) GreaterOrEq(o *ProposalNum) bool {
	if p.N > o.N {
		return true
	}
	if p.N < o.N {
		return false
	}
	return p.ProposerId >= o.ProposerId
}

func (s *Acceptor) Prepare(c context.Context, req *PkvRequest) (*PkvResponse, error) {
	pretty.Logf("[Acceptor] receive PREPARE request: %s", req.MyString())

	pVer, err := s.getLockedVersion(req.Id)
	if err != nil {
		return nil, err
	}

	defer pVer.lock.Unlock()
	pretty.Logf("[Acceptor] got version: %s", pVer)

	// Update the "last seen round" in this key version (instance).
	// We do this first because we'll return pVer.pLastRnd to the proposer later.
	// In this way the proposer will always see the most up-to-dated LastRnd.
	// 1. If the proposer's Rnd is >= than LastRnd, the reponse  will include
	//    proposer's Rnd. Proposer will still regard this as a success.
	// 2. If the proposer's Rnd is < than LastRnd, the response will not be
	//    updated. Proposer will then fail this prepare attempt.
	if req.Rnd.GreaterOrEq(pVer.pLastRnd) {
		pretty.Logf("[Acceptor] update LastRnd in this version to: %s", RndStr(req.Rnd))
		pVer.pLastRnd = req.Rnd
	}

	resp := &PkvResponse{
		Type:    PkvMsgType_PREPARE,
		LastRnd: pVer.pLastRnd,
		Val:     pVer.pVal,
		VRnd:    pVer.pVRnd,
	}

	pretty.Logf("[Acceptor] send PREPARE response: %s", resp.MyString())
	return resp, nil
}

func (s *Acceptor) Accept(ctx context.Context, req *PkvRequest) (*PkvResponse, error) {
	pretty.Logf("[Acceptor] receive ACCEPT request: %+v", req.MyString())

	pVer, err := s.getLockedVersion(req.Id)
	if err != nil {
		return nil, err
	}

	defer pVer.lock.Unlock()
	pretty.Logf("[Acceptor] got locked version: %s", pVer)

	// Update this version if proposer's Rnd is >= than its LastRnd.
	// Better to do it first so that proposer always receives up-to-dated LastRnd.
	if req.Rnd.GreaterOrEq(pVer.pLastRnd) {
		pVer.pLastRnd = req.Rnd
		pVer.pVal = req.Val
		pVer.pVRnd = req.Rnd
		pretty.Logf("[Acceptor] updated version to %s", pVer)

		// Update the latest version map.
		if pVer.Version > s.latest[req.Id.Key] {
			pretty.Logf("[Acceptor] Latest version of key %v is now %d", req.Id.Key, pVer.Version)
			s.latest[req.Id.Key] = pVer.Version
		}

		// Update snapshot with this version.
		// s.updateSnapshot(req.Id, pVer)
	}

	resp := &PkvResponse{
		Type:    PkvMsgType_ACCEPT,
		LastRnd: pVer.pLastRnd,
	}

	pretty.Logf("[Acceptor] send ACCEPT response: %v", resp.MyString())
	return resp, nil
}

func (s *Acceptor) Commit(ctx context.Context, req *PkvRequest) (*PkvResponse, error) {
	// TODO: implement Commit.
	pretty.Logf("[Acceptor] COMMIT not implemented yet")
	return nil, nil
}

func (s *Acceptor) updateSnapshot(id *InstanceId, ver *Version) bool {
	s.snapshot.lock.Lock()
	defer s.snapshot.lock.Unlock()

	pretty.Logf("[updateSnapshot] instance %v, version %s", id, ver)
	key := id.Key
	vval, found := s.snapshot.vvals[key]
	if !found {
		s.snapshot.vvals[key] = &VersionedValue{}
	}
	if vval.Version > ver.Version {
		// We cannot update snapshot with an older version.
		pretty.Logf("[updateSnapshot] Cannot update with an older version (%d -> %d)", vval.Version, ver.Version)
		return false
	}
	pretty.Logf("[updateSnapshot] [%s] value=%d, version=%d", key, ver.pVal.V, ver.Version)
	s.snapshot.vvals[key].V = ver.pVal.V
	s.snapshot.vvals[key].Version = ver.Version

	// Update the "latest" map.
	// TODO: do we need this at all? It seems to be duplicated with snapshot.Version.
	pretty.Logf("[updateSnapshot] [%s] latest version %d", key, ver.Version)
	s.latest[key] = ver.Version

	return true
}

func (s *Acceptor) GetKeyFromSnapshot(key string) (int64, bool) {
	s.snapshot.lock.Lock()
	defer s.snapshot.lock.Unlock()

	vval, found := s.snapshot.vvals[key]
	if !found {
		return 0, false
	}
	return vval.V, true
}

type Proposer struct {
	Id  *InstanceId
	Rnd *ProposalNum
	Val *Value
}

const (
	RPC_PrepareReq = iota
	RPC_AcceptReq  = iota
	RPC_CommitReq  = iota
)

func (p *Proposer) sendRequests(rpcType PkvMsgType) []*PkvResponse {
	responses := []*PkvResponse{}
	for _, addr := range ServerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			pretty.Logf("[RPC] cannot connect to %s", addr)
			// TODO: should we abort?
		}
		defer conn.Close()
		client := NewPkvClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var req *PkvRequest = &PkvRequest{
			Type: rpcType,
			Id:   p.Id,
			Rnd:  p.Rnd,
			Val:  p.Val,
		}
		pretty.Logf("[RPC] request %s to server %s", rpcType.String(), addr)
		var resp *PkvResponse
		if rpcType == PkvMsgType_PREPARE {
			resp, err = client.Prepare(ctx, req)
		} else if rpcType == PkvMsgType_ACCEPT {
			resp, err = client.Accept(ctx, req)
		} else if rpcType == PkvMsgType_COMMIT {
			// TODO: implement Commit.
			pretty.Logf("[RPC] invalid message type: %v", rpcType)
		}
		if err != nil {
			pretty.Logf("[RPC] failed with server %s: %v", addr, err)
		} else {
			pretty.Logf("[RPC] server %s responded %s", addr, resp.MyString())
			responses = append(responses, resp)
		}
	}
	return responses
}

func StartServers() []*grpc.Server {
	servers := []*grpc.Server{}
	for _, addr := range ServerAddrs {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("Error listening on %s: %v", addr, err)
		}
		s := grpc.NewServer()
		RegisterPkvServer(s, &Acceptor{
			KV:     map[string]Versions{},
			latest: map[string]int64{},
		})
		reflection.Register(s)
		pretty.Logf("Acceptor serving on %s", addr)
		servers = append(servers, s)
		go s.Serve(lis)
	}

	return servers
}

func copyProposalNum(dst *ProposalNum, src *ProposalNum) error {
	if src == nil || dst == nil {
		return ErrInvalidPointer
	}
	dst.N = src.N
	dst.ProposerId = src.ProposerId
	return nil
}

// Phase-1 returns:
// - previously accepted value with the highest proposal number if present
// - highest proposal number acceptors have seen
// - any error
func (p *Proposer) Phase1() (*Value, *ProposalNum, error) {
	// Send Prepare requests to all servers.
	responses := p.sendRequests(PkvMsgType_PREPARE)

	// Record the highest LastRnd from the responses in case phase-1 failed.
	highestRnd := ProposalNum{
		N:          p.Rnd.N,
		ProposerId: p.Rnd.ProposerId,
	}
	// If there is any accepted value from the responses, record the one with the
	// highest VRnd.
	numAccepted := 0
	acceptedValFound := false
	acceptedVal := Value{}
	acceptedVRnd := ProposalNum{}
	for _, r := range responses {
		pretty.Logf("[Proposer %d Phase-1] got response %s", p.Rnd.ProposerId, r.MyString())
		if !p.Rnd.GreaterOrEq(r.LastRnd) {
			// "LastRnd" from the response is larger than our Rnd.
			// Record the highest "LastRnd" from responses.
			if r.LastRnd.GreaterOrEq(&highestRnd) {
				copyProposalNum(&highestRnd, p.Rnd)
			}
			// Prepare request to this server failed, but we should still continue.
			// It is still possible that we get a quorum of success even some Prepare
			// requests are rejected.
			continue
		}
		// Also, check if there is any accepted value from the responses.
		if r.Val != nil && r.VRnd.GreaterOrEq(&acceptedVRnd) {
			acceptedVal.V = r.Val.V
			copyProposalNum(&acceptedVRnd, r.VRnd)
			acceptedValFound = true
		}
		numAccepted += 1
	}
	if numAccepted >= Quorum {
		// We got the quorum, but need to check if there is accepted value.
		// If so, we should use the accepted value instead of ours for phase-2.
		if acceptedValFound {
			// TODO: should we let the caller to update the proposing value?
			pretty.Logf("[Proposer %d Phase-1] should update proposing value to %v", p.Rnd.ProposerId, acceptedVal.V)
			// p.Val = &acceptedVal
			return &acceptedVal, nil, nil
		} else {
			return nil, nil, nil
		}
	} else {
		// Phase-1 failed, return the highest round we saw.
		pretty.Logf("[Proposer %d Phase-1] failed, highest Rnd %s", p.Rnd.ProposerId, RndStr(&highestRnd))
		return nil, &highestRnd, ErrNotEnoughQuorum
	}
}

// Phase-2 returns:
// - Highest proposal number from acceptors
// - Any error
func (p *Proposer) Phase2() (*ProposalNum, error) {
	// Send Accept requests to all servers.
	responses := p.sendRequests(PkvMsgType_ACCEPT)

	highestRnd := ProposalNum{
		N:          p.Rnd.N,
		ProposerId: p.Rnd.ProposerId,
	}
	numAccepted := 0
	for _, r := range responses {
		pretty.Logf("[Proposer %d Phase-2] got response %s", p.Rnd.ProposerId, r.MyString())
		if !p.Rnd.GreaterOrEq(r.LastRnd) {
			// Acceptor's LastRnd is higher than ours, so our request is rejected.
			// We need to record the highest LastRnd from the responses.
			if r.LastRnd.GreaterOrEq(&highestRnd) {
				copyProposalNum(&highestRnd, r.LastRnd)
			}
			continue
		}
		numAccepted += 1
	}
	if numAccepted >= Quorum {
		pretty.Logf("[Proposer %d Phase-2] got quorum %d", p.Rnd.ProposerId, numAccepted)
		return nil, nil
	} else {
		pretty.Logf("[Proposer %d Phase-2] failed, highest Rnd %s", p.Rnd.ProposerId, RndStr(&highestRnd))
		return &highestRnd, ErrNotEnoughQuorum
	}
}

func (p *Proposer) Paxos() *Value {
	for {
		// Phase 1 of Paxos...
		pAcceptedVal, pHighestRnd, err := p.Phase1()
		if err != nil {
			pretty.Logf("[Paxos] phase-1 failed, got highestRnd %s", RndStr(pHighestRnd))
			p.Rnd.N = pHighestRnd.N + 1
			continue
		}
		if pAcceptedVal == nil {
			pretty.Logf("[Paxos] phase-1 no accepted value seen, proceed with our value %v", p.Val)
		} else {
			pretty.Logf("[Paxos] phase-1 force update proposing value to %v", pAcceptedVal)
			p.Val = pAcceptedVal
		}
		if p.Val == nil {
			// No value for phase-2, this shouldn't happen.
			pretty.Logf("[Paxos] no value for phase-2")
			return nil
		}

		// Phase 2 of Paxos...
		pHighestRnd, err = p.Phase2()
		if err != nil {
			pretty.Logf("[Paxos] phase-2 failed, got highestRnd %s", RndStr(pHighestRnd))
			p.Rnd.N = pHighestRnd.N + 1
			continue
		}
		pretty.Logf("[Paxos] phase-2 succeeded with value %v", p.Val)
		return p.Val
	}
}

type Client struct {
	clientId int64
	proposer *Proposer
}

func NewClient(cid int64) *Client {
	return &Client{
		clientId: cid,
		proposer: &Proposer{
			Id: &InstanceId{},
			Rnd: &ProposalNum{
				N:          0,
				ProposerId: cid,
			},
			Val: nil,
		},
	}
}

func (c *Client) Set(key string, val int64) *Value {
	return c.SetVersion(key, val, -1)
}

func (c *Client) SetVersion(key string, val int64, ver int64) *Value {
	pretty.Logf("[Client] Set Key %s = %v (version %d)", key, val, ver)
	if c.proposer == nil {
		c.proposer = &Proposer{
			Id: &InstanceId{},
		}
	}
	c.proposer.Id.Key = key
	c.proposer.Id.Version = ver
	c.proposer.Val = &Value{
		V: val,
	}
	c.proposer.Rnd = &ProposalNum{
		N:          0,
		ProposerId: c.clientId,
	}

	pVal := c.proposer.Paxos()
	pretty.Logf("[Client] Set actual value %v", pVal)

	return pVal
}
