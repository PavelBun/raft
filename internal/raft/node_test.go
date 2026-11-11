package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type memoryStorage struct {
	state PersistentState
	fail  bool
}

func (s *memoryStorage) Load() (PersistentState, error) { return s.state, nil }
func (s *memoryStorage) Save(state PersistentState) error {
	if s.fail {
		return errors.New("disk unavailable")
	}
	s.state = state
	return nil
}

type offlineTransport struct{}

func (offlineTransport) RequestVote(context.Context, string, VoteRequest) (VoteResponse, error) {
	return VoteResponse{}, errors.New("offline")
}
func (offlineTransport) Heartbeat(context.Context, string, HeartbeatRequest) (HeartbeatResponse, error) {
	return HeartbeatResponse{}, errors.New("offline")
}

func testNode(t *testing.T, storage Storage) *Node {
	t.Helper()
	n, err := New(Config{ID: "n1", Peers: map[string]string{"n2": "http://n2", "n3": "http://n3"}, ElectionMin: 150 * time.Millisecond, ElectionMax: 300 * time.Millisecond, Heartbeat: 40 * time.Millisecond}, storage, offlineTransport{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestOneVotePerTermAndPersistence(t *testing.T) {
	storage := FileStorage{Path: filepath.Join(t.TempDir(), "state.json")}
	n := testNode(t, storage)
	res, err := n.HandleVote(VoteRequest{Term: 4, CandidateID: "n2"})
	if err != nil || !res.VoteGranted {
		t.Fatalf("first vote: %+v, %v", res, err)
	}
	n = testNode(t, storage) // Simulate restarting the process with the same data.
	res, err = n.HandleVote(VoteRequest{Term: 4, CandidateID: "n3"})
	if err != nil || res.VoteGranted {
		t.Fatalf("double vote after restart: %+v, %v", res, err)
	}
	res, err = n.HandleVote(VoteRequest{Term: 4, CandidateID: "n2"})
	if err != nil || !res.VoteGranted {
		t.Fatalf("idempotent vote: %+v, %v", res, err)
	}
	res, err = n.HandleVote(VoteRequest{Term: 5, CandidateID: "n3"})
	if err != nil || !res.VoteGranted || res.Term != 5 {
		t.Fatalf("next term: %+v, %v", res, err)
	}
}

func TestConcurrentVotes(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	var wg sync.WaitGroup
	results := make(chan string, 40)
	for i := 0; i < 40; i++ {
		candidate := fmt.Sprintf("n%d", 2+i%2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := n.HandleVote(VoteRequest{Term: 1, CandidateID: candidate})
			if err != nil {
				t.Error(err)
			}
			if res.VoteGranted {
				results <- candidate
			}
		}()
	}
	wg.Wait()
	close(results)
	winner := ""
	for id := range results {
		if winner != "" && winner != id {
			t.Fatalf("voted for both %s and %s", winner, id)
		}
		winner = id
	}
	if winner == "" {
		t.Fatal("no votes granted")
	}
}

func TestHeartbeatAndStaleTerms(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	_, _ = n.HandleVote(VoteRequest{Term: 3, CandidateID: "n2"})
	n.mu.Lock()
	n.role = Candidate
	n.mu.Unlock()
	res, err := n.HandleHeartbeat(HeartbeatRequest{Term: 3, LeaderID: "n2"})
	if err != nil || !res.Success || n.Status().Role != Follower || n.Status().VotedFor != "n2" {
		t.Fatalf("same-term heartbeat: %+v %v", res, err)
	}
	n.mu.Lock()
	deadline := n.deadline
	n.mu.Unlock()
	res, _ = n.HandleHeartbeat(HeartbeatRequest{Term: 2, LeaderID: "n3"})
	n.mu.Lock()
	after := n.deadline
	n.mu.Unlock()
	if res.Success || res.Term != 3 || !deadline.Equal(after) {
		t.Fatal("stale heartbeat accepted or reset timer")
	}
	res, _ = n.HandleHeartbeat(HeartbeatRequest{Term: 6, LeaderID: "n3"})
	status := n.Status()
	if !res.Success || status.Term != 6 || status.VotedFor != "" || status.LeaderID != "n3" {
		t.Fatalf("higher term: %+v", status)
	}
	vote, _ := n.HandleVote(VoteRequest{Term: 5, CandidateID: "n2"})
	if vote.VoteGranted || vote.Term != 6 {
		t.Fatal("stale vote accepted")
	}
}

func TestStorageFailureStopsVoting(t *testing.T) {
	n := testNode(t, &memoryStorage{fail: true})
	res, err := n.HandleVote(VoteRequest{Term: 1, CandidateID: "n2"})
	if err == nil || res.VoteGranted || n.Status().Error == "" {
		t.Fatal("vote granted without durable state")
	}
	if _, err := n.HandleHeartbeat(HeartbeatRequest{Term: 2, LeaderID: "n3"}); err == nil {
		t.Fatal("failed node continued participating")
	}
}

func TestHTTPValidation(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	server := httptest.NewServer(n.Handler())
	defer server.Close()
	client := HTTPTransport{Client: &http.Client{Timeout: time.Second}}
	res, err := client.RequestVote(context.Background(), server.URL, VoteRequest{Term: 1, CandidateID: "n2"})
	if err != nil || !res.VoteGranted {
		t.Fatalf("RPC: %+v %v", res, err)
	}
	if _, err := client.RequestVote(context.Background(), server.URL, VoteRequest{Term: 99, CandidateID: "stranger"}); err == nil {
		t.Fatal("unknown member accepted")
	}
	if n.Status().Term != 1 {
		t.Fatal("unknown member changed term")
	}
	request := httptest.NewRequest(http.MethodPost, "/raft/request-vote", nil)
	recorder := httptest.NewRecorder()
	n.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 400 {
		t.Fatalf("empty JSON: %d", recorder.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "/status", nil)
	recorder = httptest.NewRecorder()
	n.Handler().ServeHTTP(recorder, request)
	if recorder.Code != 405 {
		t.Fatalf("status wrong method: %d", recorder.Code)
	}
}

// Real HTTP servers exercise transport, timers, election and failover together.
func newCluster(t *testing.T, size int) ([]*Node, []func()) {
	t.Helper()
	servers := make([]*httptest.Server, size)
	for i := range servers {
		servers[i] = httptest.NewUnstartedServer(nil)
	}
	nodes := make([]*Node, size)
	stops := make([]func(), size)
	for i := range nodes {
		peers := make(map[string]string)
		for j := range servers {
			if i != j {
				peers[fmt.Sprintf("n%d", j+1)] = "http://" + servers[j].Listener.Addr().String()
			}
		}
		n, err := New(Config{ID: fmt.Sprintf("n%d", i+1), Peers: peers, ElectionMin: 250 * time.Millisecond, ElectionMax: 500 * time.Millisecond, Heartbeat: 50 * time.Millisecond}, &memoryStorage{}, HTTPTransport{Client: &http.Client{Timeout: 100 * time.Millisecond}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		servers[i].Config.Handler = n.Handler()
		servers[i].Start()
	}
	for i, n := range nodes {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- n.Run(ctx) }()
		server := servers[i]
		var once sync.Once
		stops[i] = func() {
			once.Do(func() {
				cancel()
				server.Close()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(stops[i])
	}
	return nodes, stops
}

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not reached within 5 seconds")
}

func TestClusterElectionAndFailover(t *testing.T) {
	nodes, stops := newCluster(t, 3)
	leader := -1
	waitFor(t, func() bool {
		count := 0
		for i, n := range nodes {
			if n.Status().Role == Leader {
				count++
				leader = i
			}
		}
		if count != 1 {
			return false
		}
		for _, n := range nodes {
			if n.Status().LeaderID != nodes[leader].cfg.ID {
				return false
			}
		}
		return true
	})
	oldTerm := nodes[leader].Status().Term
	// With heartbeats arriving, the cluster should keep the same leader and term.
	time.Sleep(700 * time.Millisecond)
	for _, n := range nodes {
		if n.Status().Term != oldTerm {
			t.Fatal("unstable term despite heartbeats")
		}
	}
	stops[leader]()
	newLeader := -1
	waitFor(t, func() bool {
		count := 0
		for i, n := range nodes {
			if i != leader && n.Status().Role == Leader {
				count++
				newLeader = i
			}
		}
		return count == 1 && nodes[newLeader].Status().Term > oldTerm
	})
	// After losing two nodes the remaining follower must not elect itself.
	stops[newLeader]()
	remaining := 3 - leader - newLeader
	time.Sleep(1200 * time.Millisecond)
	if nodes[remaining].Status().Role == Leader {
		t.Fatal("minority elected a leader")
	}
}

func TestSingleNodeElection(t *testing.T) {
	nodes, _ := newCluster(t, 1)
	waitFor(t, func() bool { return nodes[0].Status().Role == Leader })
	if nodes[0].Status().Votes != 1 || nodes[0].Status().Majority != 1 {
		t.Fatal("single node majority incorrect")
	}
}

type delayedTransport struct {
	started chan struct{}
	reply   chan VoteResponse
}

func (d delayedTransport) RequestVote(ctx context.Context, _ string, _ VoteRequest) (VoteResponse, error) {
	close(d.started)
	select {
	case res := <-d.reply:
		return res, nil
	case <-ctx.Done():
		return VoteResponse{}, ctx.Err()
	}
}

func (d delayedTransport) Heartbeat(context.Context, string, HeartbeatRequest) (HeartbeatResponse, error) {
	return HeartbeatResponse{}, errors.New("offline")
}

func TestLateVoteCannotWinNewTerm(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	d := delayedTransport{started: make(chan struct{}), reply: make(chan VoteResponse, 1)}
	n.transport = d
	n.state = PersistentState{Term: 1, VotedFor: "n1"}
	n.role, n.votes = Candidate, 1
	done := make(chan struct{})
	go func() { n.requestVote(context.Background(), "http://n2", 1); close(done) }()
	<-d.started
	_, _ = n.HandleHeartbeat(HeartbeatRequest{Term: 2, LeaderID: "n3"})
	d.reply <- VoteResponse{Term: 1, VoteGranted: true}
	<-done
	if status := n.Status(); status.Term != 2 || status.Role != Follower || status.LeaderID != "n3" {
		t.Fatalf("late vote changed state: %+v", status)
	}
}

func TestHigherTermResponseStepsDown(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	d := delayedTransport{started: make(chan struct{}), reply: make(chan VoteResponse, 1)}
	n.transport = d
	n.state = PersistentState{Term: 1, VotedFor: "n1"}
	n.role, n.leaderID = Leader, "n1"
	d.reply <- VoteResponse{Term: 8}
	n.requestVote(context.Background(), "http://n2", 1)
	if status := n.Status(); status.Term != 8 || status.Role != Follower || status.VotedFor != "" {
		t.Fatalf("higher-term reply ignored: %+v", status)
	}
}

func TestElectionRetriesWithoutQuorum(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	waitFor(t, func() bool { return n.Status().Term >= 3 })
	status := n.Status()
	if status.Role != Candidate || status.Votes != 1 {
		t.Fatalf("minority won: %+v", status)
	}
}
