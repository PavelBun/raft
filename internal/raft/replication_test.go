package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// makeLog строит журнал с индексами подряд; аргументы — сроки записей.
func makeLog(terms ...uint64) []Entry {
	log := make([]Entry, len(terms))
	for i, term := range terms {
		log[i] = Entry{Index: uint64(i) + 1, Term: term, Command: fmt.Sprintf("c%d", i+1)}
	}
	return log
}

func logTerms(log []Entry) []uint64 {
	terms := make([]uint64, len(log))
	for i, e := range log {
		terms[i] = e.Term
	}
	return terms
}

func nodeWith(t *testing.T, id string, term uint64, log []Entry) *Node {
	t.Helper()
	peers := make(map[string]string)
	for _, other := range []string{"n1", "n2", "n3"} {
		if other != id {
			peers[other] = other
		}
	}
	n, err := New(Config{ID: id, Peers: peers, ElectionMin: 150 * time.Millisecond, ElectionMax: 300 * time.Millisecond, Heartbeat: 40 * time.Millisecond},
		&memoryStorage{state: PersistentState{Term: term, Log: log}}, offlineTransport{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// directTransport вызывает обработчики узлов напрямую, без сети и таймеров.
type directTransport map[string]*Node

func (d directTransport) RequestVote(_ context.Context, address string, req VoteRequest) (VoteResponse, error) {
	return d[address].HandleVote(req)
}
func (d directTransport) AppendEntries(_ context.Context, address string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	return d[address].HandleAppendEntries(req)
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	n := testNode(t, &memoryStorage{state: PersistentState{Term: 3, Log: makeLog(1, 2)}})
	shorter, _ := n.HandleVote(VoteRequest{Term: 4, CandidateID: "n2", LastLogIndex: 1, LastLogTerm: 2})
	olderTerm, _ := n.HandleVote(VoteRequest{Term: 5, CandidateID: "n2", LastLogIndex: 9, LastLogTerm: 1})
	if shorter.VoteGranted || olderTerm.VoteGranted {
		t.Fatalf("stale candidate got a vote: %+v %+v", shorter, olderTerm)
	}
	equal, _ := n.HandleVote(VoteRequest{Term: 6, CandidateID: "n2", LastLogIndex: 2, LastLogTerm: 2})
	newerTerm, _ := n.HandleVote(VoteRequest{Term: 7, CandidateID: "n3", LastLogIndex: 1, LastLogTerm: 3})
	if !equal.VoteGranted || !newerTerm.VoteGranted {
		t.Fatalf("up-to-date candidate refused: %+v %+v", equal, newerTerm)
	}
}

func TestDeniedVoteDoesNotDelayElection(t *testing.T) {
	n := testNode(t, &memoryStorage{state: PersistentState{Term: 3, Log: makeLog(1, 2)}})
	before := n.deadline
	time.Sleep(time.Millisecond)
	res, _ := n.HandleVote(VoteRequest{Term: 3, CandidateID: "n2"})
	if res.VoteGranted || !n.deadline.Equal(before) {
		t.Fatal("denied vote reset election timer")
	}
}

func TestHigherTermKeepsLog(t *testing.T) {
	storage := &memoryStorage{state: PersistentState{Term: 1, Log: makeLog(1, 1)}}
	n := testNode(t, storage)
	if _, err := n.HandleVote(VoteRequest{Term: 5, CandidateID: "n2"}); err != nil {
		t.Fatal(err)
	}
	if len(n.Log().Entries) != 2 || len(storage.state.Log) != 2 {
		t.Fatal("higher term erased the log")
	}
}

func TestAppendEntriesRules(t *testing.T) {
	storage := FileStorage{Path: filepath.Join(t.TempDir(), "state.json")}
	n := testNode(t, storage)
	send := func(req AppendEntriesRequest) bool {
		t.Helper()
		req.LeaderID = "n2"
		res, err := n.HandleAppendEntries(req)
		if err != nil {
			t.Fatal(err)
		}
		return res.Success
	}
	if !send(AppendEntriesRequest{Term: 1, Entries: makeLog(1, 1, 1)}) {
		t.Fatal("first entries refused")
	}
	// Пункт 2: журнал слишком короткий или срок на PrevLogIndex другой.
	if send(AppendEntriesRequest{Term: 1, PrevLogIndex: 5, PrevLogTerm: 1}) {
		t.Fatal("gap accepted")
	}
	if send(AppendEntriesRequest{Term: 2, PrevLogIndex: 3, PrevLogTerm: 2}) {
		t.Fatal("term mismatch accepted")
	}
	if len(n.Log().Entries) != 3 || n.Status().LeaderID != "n2" {
		t.Fatal("rejected request changed the log or lost the leader")
	}
	// Пункт 3: конфликт удаляет запись и всё после неё.
	if !send(AppendEntriesRequest{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Index: 2, Term: 2, Command: "new"}}}) {
		t.Fatal("conflict refused")
	}
	if got := logTerms(n.state.Log); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("conflict not resolved: %v", got)
	}
	// Пункт 4: запоздавший старый запрос не обрезает журнал.
	if !send(AppendEntriesRequest{Term: 2, Entries: makeLog(1)}) || len(n.Log().Entries) != 2 {
		t.Fatal("old request truncated the log")
	}
	// Пункт 5: commitIndex ограничен последней новой записью и не уменьшается.
	send(AppendEntriesRequest{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 9})
	if s := n.Status(); s.CommitIndex != 1 || s.LastApplied != 1 {
		t.Fatalf("commit past verified prefix: %+v", s)
	}
	send(AppendEntriesRequest{Term: 2, PrevLogIndex: 2, PrevLogTerm: 2, LeaderCommit: 9})
	send(AppendEntriesRequest{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 9})
	if s := n.Status(); s.CommitIndex != 2 || s.LastApplied != 2 {
		t.Fatalf("commit index: %+v", s)
	}
	// Журнал переживает перезапуск, а commitIndex — изменчивое состояние.
	n = testNode(t, storage)
	if view := n.Log(); len(view.Entries) != 2 || view.Entries[1].Command != "new" || view.CommitIndex != 0 {
		t.Fatalf("after restart: %+v", view)
	}
}

func TestAppendEntriesCannotOverwriteCommitted(t *testing.T) {
	n := testNode(t, &memoryStorage{state: PersistentState{Term: 1, Log: makeLog(1, 1)}})
	n.commitIndex = 2
	_, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "n2", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Index: 2, Term: 2}}})
	if err == nil || len(n.Log().Entries) != 2 || n.state.Log[1].Term != 1 {
		t.Fatalf("committed entry overwritten: %v", err)
	}
}

func TestBadAppendEntriesRejected(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	for _, req := range []AppendEntriesRequest{
		{Term: 0, LeaderID: "n2"},
		{Term: 2, LeaderID: "n2", PrevLogIndex: 1},
		{Term: 2, LeaderID: "n2", PrevLogTerm: 1},
		{Term: 2, LeaderID: "n2", Entries: []Entry{{Index: 2, Term: 1}}},
		{Term: 2, LeaderID: "n2", Entries: []Entry{{Index: 1, Term: 3}}},
		{Term: 2, LeaderID: "n2", Entries: []Entry{{Index: 1, Term: 2}, {Index: 2, Term: 1}}},
	} {
		if _, err := n.HandleAppendEntries(req); err == nil {
			t.Fatalf("accepted %+v", req)
		}
	}
}

func TestLeaderCommitsOnlyCurrentTermByCounting(t *testing.T) {
	n := nodeWith(t, "n1", 3, makeLog(1, 2))
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.matchIndex["n2"] = 2 // Запись срока 2 есть у большинства, но срок сейчас 3.
	n.advanceCommitLocked()
	n.mu.Unlock()
	if n.Status().CommitIndex != 0 {
		t.Fatal("entry of a previous term committed by counting replicas")
	}
	index, term, err := n.Propose([]string{"x"})
	if err != nil || index != 3 || term != 3 {
		t.Fatalf("propose: %d %d %v", index, term, err)
	}
	if n.Status().CommitIndex != 0 {
		t.Fatal("committed without a majority")
	}
	n.mu.Lock()
	n.matchIndex["n2"] = 3
	n.advanceCommitLocked()
	n.mu.Unlock()
	// Запись текущего срока фиксирует и все предыдущие.
	if s := n.Status(); s.CommitIndex != 3 || s.LastApplied != 3 {
		t.Fatalf("current-term entry not committed: %+v", s)
	}
}

func TestLeaderRepairsDivergedFollower(t *testing.T) {
	leader := nodeWith(t, "n1", 3, makeLog(1, 1, 2, 2, 3))
	follower := nodeWith(t, "n2", 2, makeLog(1, 1, 1, 1, 1, 1, 1))
	leader.transport = directTransport{"n2": follower}
	leader.mu.Lock()
	leader.becomeLeaderLocked()
	leader.mu.Unlock()
	leader.replicateTo(context.Background(), "n2", "n2", 3)
	if got := fmt.Sprint(logTerms(follower.state.Log)); got != "[1 1 2 2 3]" {
		t.Fatalf("follower log: %s", got)
	}
	s := leader.Status()
	if s.NextIndex["n2"] != 6 || s.MatchIndex["n2"] != 5 || s.CommitIndex != 5 {
		t.Fatalf("leader state: %+v", s)
	}
	// О фиксации follower узнаёт из следующего AppendEntries.
	leader.replicateTo(context.Background(), "n2", "n2", 3)
	if follower.Status().CommitIndex != 5 {
		t.Fatal("follower did not learn commit index")
	}
}

func TestLaggingFollowerCatchesUpInBatches(t *testing.T) {
	terms := make([]uint64, 3*maxEntriesPerRPC+1)
	for i := range terms {
		terms[i] = 1
	}
	leader := nodeWith(t, "n1", 1, makeLog(terms...))
	follower := nodeWith(t, "n2", 1, nil)
	leader.transport = directTransport{"n2": follower}
	leader.mu.Lock()
	leader.becomeLeaderLocked()
	leader.nextIndex["n2"] = 1
	leader.mu.Unlock()
	leader.replicateTo(context.Background(), "n2", "n2", 1)
	if len(follower.state.Log) != len(terms) {
		t.Fatalf("follower has %d of %d entries", len(follower.state.Log), len(terms))
	}
}

func TestOutOfOrderResponses(t *testing.T) {
	n := nodeWith(t, "n1", 1, makeLog(1, 1, 1, 1, 1))
	n.mu.Lock()
	defer n.mu.Unlock()
	n.becomeLeaderLocked()
	// Опоздавший успех с меньшим индексом не откатывает прогресс.
	n.handleAppendEntriesResponseLocked("n2", AppendEntriesRequest{Term: 1, PrevLogIndex: 3, Entries: makeLog(1, 1)[:2]}, AppendEntriesResponse{Term: 1, Success: true})
	n.handleAppendEntriesResponseLocked("n2", AppendEntriesRequest{Term: 1, PrevLogIndex: 1}, AppendEntriesResponse{Term: 1, Success: true})
	if n.matchIndex["n2"] != 5 || n.nextIndex["n2"] != 6 {
		t.Fatalf("progress went back: match %d next %d", n.matchIndex["n2"], n.nextIndex["n2"])
	}
	// Два отказа на один и тот же запрос уменьшают nextIndex только один раз.
	req := AppendEntriesRequest{Term: 1, PrevLogIndex: 5}
	first := n.handleAppendEntriesResponseLocked("n3", req, AppendEntriesResponse{Term: 1})
	second := n.handleAppendEntriesResponseLocked("n3", req, AppendEntriesResponse{Term: 1})
	if !first || second || n.nextIndex["n3"] != 5 {
		t.Fatalf("double decrement: %v %v next %d", first, second, n.nextIndex["n3"])
	}
}

func TestHigherTermInAppendResponseStepsDown(t *testing.T) {
	n := nodeWith(t, "n1", 1, makeLog(1))
	n.mu.Lock()
	n.becomeLeaderLocked()
	again := n.handleAppendEntriesResponseLocked("n2", AppendEntriesRequest{Term: 1}, AppendEntriesResponse{Term: 4})
	n.mu.Unlock()
	if s := n.Status(); again || s.Role != Follower || s.Term != 4 || s.NextIndex != nil || s.LastLogIndex != 1 {
		t.Fatalf("leader did not step down: %+v", s)
	}
}

func TestStoredLogIsValidated(t *testing.T) {
	bad := []PersistentState{
		{Term: 1, Log: []Entry{{Index: 2, Term: 1}}},
		{Term: 1, Log: makeLog(2)},
		{Term: 2, Log: makeLog(2, 1)},
		{Term: 1, Log: makeLog(0)},
	}
	for _, state := range bad {
		_, err := New(Config{ID: "n1", ElectionMin: 150 * time.Millisecond, ElectionMax: 300 * time.Millisecond, Heartbeat: 40 * time.Millisecond},
			&memoryStorage{state: state}, offlineTransport{}, nil)
		if err == nil {
			t.Fatalf("accepted broken log: %+v", state)
		}
	}
}

func TestEntryLostWhenReplacedByNewLeader(t *testing.T) {
	n := nodeWith(t, "n1", 1, nil)
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()
	index, term, err := n.Propose([]string{"lost"})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- n.WaitCommitted(context.Background(), index, term) }()
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "n2", Entries: []Entry{{Index: 1, Term: 2, Command: "winner"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrEntryLost) {
			t.Fatalf("wait result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not woken")
	}
}

func waitLeader(t *testing.T, nodes []*Node, skip map[int]bool) int {
	t.Helper()
	leader := -1
	waitFor(t, func() bool {
		count := 0
		for i, n := range nodes {
			if !skip[i] && n.Status().Role == Leader {
				count++
				leader = i
			}
		}
		return count == 1
	})
	return leader
}

func mustCommit(t *testing.T, n *Node, commands ...string) {
	t.Helper()
	index, term, err := n.Propose(commands)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := n.WaitCommitted(ctx, index, term); err != nil {
		t.Fatal(err)
	}
}

func commands(n *Node) string {
	view := n.Log()
	list := make([]string, len(view.Entries))
	for i, e := range view.Entries {
		list[i] = e.Command
	}
	return strings.Join(list, ",")
}

func TestClusterReplicatesAndSurvivesFailover(t *testing.T) {
	nodes, stops := newCluster(t, 3)
	leader := waitLeader(t, nodes, nil)
	mustCommit(t, nodes[leader], "a", "b")
	mustCommit(t, nodes[leader], "c")
	waitFor(t, func() bool {
		for _, n := range nodes {
			if n.Status().CommitIndex != 3 || commands(n) != "a,b,c" {
				return false
			}
		}
		return true
	})
	// Новый лидер обязан иметь все зафиксированные записи и продолжает журнал.
	stops[leader]()
	second := waitLeader(t, nodes, map[int]bool{leader: true})
	mustCommit(t, nodes[second], "d")
	waitFor(t, func() bool {
		for i, n := range nodes {
			if i != leader && (n.Status().CommitIndex != 4 || commands(n) != "a,b,c,d") {
				return false
			}
		}
		return true
	})
}

func TestNoCommitWithoutMajority(t *testing.T) {
	nodes, stops := newCluster(t, 3)
	leader := waitLeader(t, nodes, nil)
	mustCommit(t, nodes[leader], "a")
	for i := range nodes {
		if i != leader {
			stops[i]()
		}
	}
	index, term, err := nodes[leader].Propose([]string{"b"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := nodes[leader].WaitCommitted(ctx, index, term); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait: %v", err)
	}
	if s := nodes[leader].Status(); s.CommitIndex != 1 || s.LastLogIndex != 2 {
		t.Fatalf("committed without majority: %+v", s)
	}
}

func post(handler http.Handler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/log", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestLogAPI(t *testing.T) {
	nodes, _ := newCluster(t, 1)
	waitLeader(t, nodes, nil)
	handler := nodes[0].Handler()
	if r := post(handler, `{"commands":["a","b"]}`); r.Code != 200 || !strings.Contains(r.Body.String(), `"last_index":2`) {
		t.Fatalf("add entries: %d %s", r.Code, r.Body)
	}
	if r := post(handler, `{"command":"c"}`); r.Code != 200 || !strings.Contains(r.Body.String(), `"first_index":3`) {
		t.Fatalf("add entry: %d %s", r.Code, r.Body)
	}
	for _, body := range []string{`{}`, `{"command":"x","commands":["y"]}`, `{"commands":[""]}`, `{"command":"` + strings.Repeat("x", maxCommandBytes+1) + `"}`, `{"unknown":1}`} {
		if r := post(handler, body); r.Code != 400 {
			t.Fatalf("bad body accepted: %d", r.Code)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/log", nil))
	var view LogView
	if err := json.Unmarshal(recorder.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != 200 || len(view.Entries) != 3 || view.Entries[2].Command != "c" || !view.Entries[2].Committed || view.CommitIndex != 3 {
		t.Fatalf("show log: %d %+v", recorder.Code, view)
	}
}

func TestFollowerPointsClientToLeader(t *testing.T) {
	n := testNode(t, &memoryStorage{})
	if r := post(n.Handler(), `{"command":"x"}`); r.Code != 409 {
		t.Fatalf("no leader known: %d", r.Code)
	}
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatal(err)
	}
	r := post(n.Handler(), `{"command":"x"}`)
	if r.Code != 409 || !strings.Contains(r.Body.String(), `"leader_address":"http://n2"`) {
		t.Fatalf("redirect: %d %s", r.Code, r.Body)
	}
	if len(n.Log().Entries) != 0 {
		t.Fatal("follower accepted a client command")
	}
}
