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
	"strings"
	"testing"
	"time"
)

// makeLog строит журнал с индексами подряд; аргументы — сроки записей.
func makeLog(terms ...uint64) []Entry {
	log := make([]Entry, len(terms))
	for i, term := range terms {
		log[i] = Entry{Index: uint64(i) + 1, Term: term, Command: fmt.Sprintf("set k%d v%d", i+1, i+1)}
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

// nodeWith создаёт узел кластера n1..n3 с заданным сроком и журналом.
func nodeWith(t *testing.T, id string, term uint64, log []Entry) *Node {
	t.Helper()
	peers := make(map[string]string)
	for _, other := range []string{"n1", "n2", "n3"} {
		if other != id {
			peers[other] = other
		}
	}
	n, err := New(Config{ID: id, Peers: peers, ElectionMin: 150 * time.Millisecond, ElectionMax: 300 * time.Millisecond, Heartbeat: 40 * time.Millisecond},
		offlineTransport{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	n.state = PersistentState{Term: term, Log: log}
	return n
}

// directTransport вызывает обработчики узлов напрямую, без сети и таймеров,
// и считает отправленные AppendEntries.
type directTransport struct {
	nodes map[string]*Node
	calls *int
}

func (d directTransport) RequestVote(_ context.Context, address string, req VoteRequest) (VoteResponse, error) {
	return d.nodes[address].HandleVote(req)
}
func (d directTransport) AppendEntries(_ context.Context, address string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	*d.calls++
	return d.nodes[address].HandleAppendEntries(req)
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	n := nodeWith(t, "n1", 3, makeLog(1, 2))
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
	n := nodeWith(t, "n1", 3, makeLog(1, 2))
	before := n.deadline
	time.Sleep(time.Millisecond)
	res, _ := n.HandleVote(VoteRequest{Term: 3, CandidateID: "n2"})
	if res.VoteGranted || !n.deadline.Equal(before) {
		t.Fatal("denied vote reset election timer")
	}
}

func TestHigherTermKeepsLog(t *testing.T) {
	n := nodeWith(t, "n1", 1, makeLog(1, 1))
	if _, err := n.HandleVote(VoteRequest{Term: 5, CandidateID: "n2"}); err != nil {
		t.Fatal(err)
	}
	if len(n.Log().Entries) != 2 {
		t.Fatal("higher term erased the log")
	}
}

func TestAppendEntriesRules(t *testing.T) {
	n := testNode(t)
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
	if !send(AppendEntriesRequest{Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Index: 2, Term: 2, Command: "set x new"}}}) {
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
	// Применённые записи выполнены машиной состояний follower.
	if data := n.KV().Data; data["k1"] != "v1" || data["x"] != "new" {
		t.Fatalf("state machine: %v", data)
	}
}

func TestAppendEntriesCannotOverwriteCommitted(t *testing.T) {
	n := nodeWith(t, "n1", 1, makeLog(1, 1))
	n.commitIndex = 2
	_, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "n2", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Index: 2, Term: 2}}})
	if err == nil || len(n.Log().Entries) != 2 || n.state.Log[1].Term != 1 {
		t.Fatalf("committed entry overwritten: %v", err)
	}
}

func TestBadAppendEntriesRejected(t *testing.T) {
	n := testNode(t)
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
	pending, err := n.Propose([]string{"get k1"})
	if err != nil {
		t.Fatal(err)
	}
	if n.Status().CommitIndex != 0 || len(pending[0]) != 0 {
		t.Fatal("committed without a majority")
	}
	n.mu.Lock()
	n.matchIndex["n2"] = 3
	n.advanceCommitLocked()
	n.mu.Unlock()
	// Запись текущего срока фиксирует и все предыдущие; клиент получает результат.
	if s := n.Status(); s.CommitIndex != 3 || s.LastApplied != 3 {
		t.Fatalf("current-term entry not committed: %+v", s)
	}
	if res := <-pending[0]; res.Err != nil || res.Index != 3 || !res.Found || res.Value != "v1" {
		t.Fatalf("client result: %+v", res)
	}
}

func TestFollowerConflictHint(t *testing.T) {
	n := nodeWith(t, "n2", 6, makeLog(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3))
	// На позиции 10 у нас срок 3, а у лидера 6: подсказываем начало срока 3.
	res, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 6, LeaderID: "n1", PrevLogIndex: 10, PrevLogTerm: 6})
	if err != nil || res.Success || res.ConflictTerm != 3 || res.ConflictIndex != 7 {
		t.Fatalf("term conflict hint: %+v %v", res, err)
	}
	// Журнал короче PrevLogIndex: подсказываем позицию сразу после своей последней записи.
	res, _ = n.HandleAppendEntries(AppendEntriesRequest{Term: 6, LeaderID: "n1", PrevLogIndex: 20, PrevLogTerm: 6})
	if res.Success || res.ConflictTerm != 0 || res.ConflictIndex != 12 {
		t.Fatalf("short log hint: %+v", res)
	}
	// Конфликт на первой записи срока, который начинается с индекса 1.
	res, _ = n.HandleAppendEntries(AppendEntriesRequest{Term: 6, LeaderID: "n1", PrevLogIndex: 2, PrevLogTerm: 4})
	if res.ConflictTerm != 1 || res.ConflictIndex != 1 {
		t.Fatalf("first term hint: %+v", res)
	}
}

// Восстановление ленивое: каждый вызов replicateTo — это один heartbeat и один RPC.
// По подсказке лидер перескакивает через целый срок, а не идёт по одной записи.
// Пример журналов — как в разборе: у follower остались записи сроков 2 и 3.
func TestLeaderRepairsDivergedFollowerLazily(t *testing.T) {
	leader := nodeWith(t, "n1", 6, makeLog(1, 1, 1, 4, 4, 5, 5, 6, 6, 6))
	follower := nodeWith(t, "n2", 3, makeLog(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3))
	calls := 0
	leader.transport = directTransport{nodes: map[string]*Node{"n2": follower}, calls: &calls}
	leader.mu.Lock()
	leader.becomeLeaderLocked()
	leader.mu.Unlock()
	heartbeat := func() { leader.replicateTo(context.Background(), "n2", "n2", 6) }
	heartbeat() // prev=10: у follower срок 3, он начинается с 7
	if calls != 1 || leader.Status().NextIndex["n2"] != 7 {
		t.Fatalf("after first rejection: calls %d next %d", calls, leader.Status().NextIndex["n2"])
	}
	heartbeat() // prev=6: у follower срок 2, он начинается с 4
	if leader.Status().NextIndex["n2"] != 4 {
		t.Fatalf("after second rejection: next %d", leader.Status().NextIndex["n2"])
	}
	if got := fmt.Sprint(logTerms(follower.state.Log)); got != "[1 1 1 2 2 2 3 3 3 3 3]" {
		t.Fatalf("follower changed before a match: %s", got)
	}
	heartbeat() // prev=3: совпало, конфликт удалён, записи лидера дописаны
	if got := fmt.Sprint(logTerms(follower.state.Log)); got != "[1 1 1 4 4 5 5 6 6 6]" || calls != 3 {
		t.Fatalf("follower log: %s after %d calls", got, calls)
	}
	s := leader.Status()
	if s.NextIndex["n2"] != 11 || s.MatchIndex["n2"] != 10 || s.CommitIndex != 10 {
		t.Fatalf("leader state: %+v", s)
	}
	// О фиксации follower узнаёт из следующего AppendEntries.
	heartbeat()
	if follower.Status().CommitIndex != 10 || follower.KV().Data["k10"] != "v10" {
		t.Fatal("follower did not learn commit index")
	}
}

// Пустой follower (например, перезапущенный узел без диска) и 10 000 записей:
// один отказ находит место, дальше журнал идёт пачками по размеру.
func TestEmptyFollowerCatchesUpInBatches(t *testing.T) {
	terms := make([]uint64, 10000)
	for i := range terms {
		terms[i] = 1
	}
	leader := nodeWith(t, "n1", 1, makeLog(terms...))
	follower := nodeWith(t, "n2", 1, nil)
	calls := 0
	leader.transport = directTransport{nodes: map[string]*Node{"n2": follower}, calls: &calls}
	leader.mu.Lock()
	leader.becomeLeaderLocked()
	leader.mu.Unlock()
	heartbeat := func() { leader.replicateTo(context.Background(), "n2", "n2", 1) }
	heartbeat()
	if leader.Status().NextIndex["n2"] != 1 || len(follower.state.Log) != 0 {
		t.Fatalf("empty follower: next %d", leader.Status().NextIndex["n2"])
	}
	heartbeat()
	first := len(follower.state.Log)
	size := 0
	for _, e := range follower.state.Log {
		size += entrySize(e)
	}
	if first <= 1000 || first >= len(terms) || size > maxBatchBytes {
		t.Fatalf("first batch: %d entries, %d bytes", first, size)
	}
	for len(follower.state.Log) < len(terms) && calls < 10 {
		heartbeat()
	}
	if len(follower.state.Log) != len(terms) || calls > 4 {
		t.Fatalf("follower has %d of %d entries after %d calls", len(follower.state.Log), len(terms), calls)
	}
}

func TestOutOfOrderResponses(t *testing.T) {
	n := nodeWith(t, "n1", 1, makeLog(1, 1, 1, 1, 1))
	n.mu.Lock()
	defer n.mu.Unlock()
	n.becomeLeaderLocked()
	// Опоздавший успех с меньшим индексом не откатывает прогресс.
	n.handleAppendEntriesResponseLocked("n2", AppendEntriesRequest{Term: 1, PrevLogIndex: 3, Entries: makeLog(1, 1)}, AppendEntriesResponse{Term: 1, Success: true})
	n.handleAppendEntriesResponseLocked("n2", AppendEntriesRequest{Term: 1, PrevLogIndex: 1}, AppendEntriesResponse{Term: 1, Success: true})
	if n.matchIndex["n2"] != 5 || n.nextIndex["n2"] != 6 {
		t.Fatalf("progress went back: match %d next %d", n.matchIndex["n2"], n.nextIndex["n2"])
	}
	// Два отказа на один и тот же запрос откатывают nextIndex только один раз.
	req := AppendEntriesRequest{Term: 1, PrevLogIndex: 5}
	hint := AppendEntriesResponse{Term: 1, ConflictIndex: 2}
	n.handleAppendEntriesResponseLocked("n3", req, hint)
	n.handleAppendEntriesResponseLocked("n3", req, hint)
	if n.nextIndex["n3"] != 2 {
		t.Fatalf("double rollback: next %d", n.nextIndex["n3"])
	}
	// Подсказка дальше PrevLogIndex или нулевая — обычный шаг на одну запись.
	n.nextIndex["n3"] = 6
	n.handleAppendEntriesResponseLocked("n3", req, AppendEntriesResponse{Term: 1, ConflictIndex: 9})
	if n.nextIndex["n3"] != 5 {
		t.Fatalf("bad hint: next %d", n.nextIndex["n3"])
	}
}

func TestHigherTermInAppendResponseStepsDown(t *testing.T) {
	n := nodeWith(t, "n1", 1, makeLog(1))
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.handleAppendEntriesResponseLocked("n2", AppendEntriesRequest{Term: 1}, AppendEntriesResponse{Term: 4})
	n.mu.Unlock()
	if s := n.Status(); s.Role != Follower || s.Term != 4 || s.NextIndex != nil || s.LastLogIndex != 1 {
		t.Fatalf("leader did not step down: %+v", s)
	}
}

func TestEntryLostWhenReplacedByNewLeader(t *testing.T) {
	n := nodeWith(t, "n1", 1, nil)
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()
	pending, err := n.Propose([]string{"set a lost"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 2, LeaderID: "n2", Entries: []Entry{{Index: 1, Term: 2, Command: "set a winner"}}, LeaderCommit: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-pending[0]:
		if !errors.Is(res.Err, ErrEntryLost) {
			t.Fatalf("wait result: %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client was not notified")
	}
	if n.KV().Data["a"] != "winner" {
		t.Fatal("state machine did not apply the new leader's entry")
	}
}

func TestStateMachine(t *testing.T) {
	kv := NewKV()
	steps := []struct {
		command string
		want    Result
	}{
		{"get a", Result{}},
		{"set a 1", Result{Value: "1", Found: true}},
		{"set b hello world", Result{Value: "hello world", Found: true}},
		{"get b", Result{Value: "hello world", Found: true}},
		{"delete a", Result{Found: true}},
		{"delete a", Result{}},
		{"get a", Result{}},
	}
	for _, step := range steps {
		if got := kv.Apply(step.command); got != step.want {
			t.Fatalf("%q: got %+v want %+v", step.command, got, step.want)
		}
	}
	for _, bad := range []string{"", "set a", "get", "get a b", "delete", "put a 1", "set  1"} {
		if _, err := ParseCommand(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
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

func mustApply(t *testing.T, n *Node, commands ...string) []ApplyResult {
	t.Helper()
	pending, err := n.Propose(commands)
	if err != nil {
		t.Fatal(err)
	}
	results := make([]ApplyResult, len(pending))
	for i, ch := range pending {
		select {
		case results[i] = <-ch:
			if results[i].Err != nil {
				t.Fatal(results[i].Err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("entry not applied")
		}
	}
	return results
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
	mustApply(t, nodes[leader], "set a 1", "set b 2")
	if res := mustApply(t, nodes[leader], "get a"); res[0].Value != "1" || !res[0].Found {
		t.Fatalf("get: %+v", res[0])
	}
	waitFor(t, func() bool {
		for _, n := range nodes {
			if n.Status().LastApplied != 3 || commands(n) != "set a 1,set b 2,get a" || n.KV().Data["b"] != "2" {
				return false
			}
		}
		return true
	})
	// Новый лидер обязан иметь все зафиксированные записи и продолжает журнал.
	stops[leader]()
	second := waitLeader(t, nodes, map[int]bool{leader: true})
	if res := mustApply(t, nodes[second], "get b"); res[0].Value != "2" {
		t.Fatalf("value lost after failover: %+v", res[0])
	}
	waitFor(t, func() bool {
		for i, n := range nodes {
			if i != leader && (n.Status().LastApplied != 4 || commands(n) != "set a 1,set b 2,get a,get b") {
				return false
			}
		}
		return true
	})
}

func TestNoCommitWithoutMajority(t *testing.T) {
	nodes, stops := newCluster(t, 3)
	leader := waitLeader(t, nodes, nil)
	mustApply(t, nodes[leader], "set a 1")
	for i := range nodes {
		if i != leader {
			stops[i]()
		}
	}
	pending, err := nodes[leader].Propose([]string{"set a 2"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-pending[0]:
		t.Fatalf("applied without majority: %+v", res)
	case <-time.After(400 * time.Millisecond):
	}
	if s := nodes[leader].Status(); s.CommitIndex != 1 || s.LastLogIndex != 2 || nodes[leader].KV().Data["a"] != "1" {
		t.Fatalf("committed without majority: %+v", s)
	}
}

func post(handler http.Handler, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/log", strings.NewReader(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func get(handler http.Handler, path string, value any) int {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	_ = json.Unmarshal(recorder.Body.Bytes(), value)
	return recorder.Code
}

func TestLogAndKVAPI(t *testing.T) {
	nodes, _ := newCluster(t, 1)
	waitLeader(t, nodes, nil)
	handler := nodes[0].Handler()
	r := post(handler, `{"commands":["set a 1","get a"]}`)
	if r.Code != 200 || !strings.Contains(r.Body.String(), `{"index":2,"term":`) || !strings.Contains(r.Body.String(), `"value":"1","found":true}]`) {
		t.Fatalf("add entries: %d %s", r.Code, r.Body)
	}
	if r := post(handler, `{"command":"delete a"}`); r.Code != 200 || !strings.Contains(r.Body.String(), `"found":true`) {
		t.Fatalf("add entry: %d %s", r.Code, r.Body)
	}
	for _, body := range []string{`{}`, `{"command":"get a","commands":["get b"]}`, `{"commands":[""]}`, `{"command":"put a 1"}`,
		`{"command":"set a ` + strings.Repeat("x", maxCommandBytes) + `"}`, `{"unknown":1}`} {
		if r := post(handler, body); r.Code != 400 {
			t.Fatalf("bad body accepted: %d", r.Code)
		}
	}
	var view LogView
	if code := get(handler, "/log", &view); code != 200 || len(view.Entries) != 3 || view.Entries[2].Command != "delete a" || !view.Entries[2].Committed {
		t.Fatalf("show log: %d %+v", code, view)
	}
	var kv KVView
	if code := get(handler, "/kv", &kv); code != 200 || kv.LastApplied != 3 || len(kv.Data) != 0 {
		t.Fatalf("show kv: %d %+v", code, kv)
	}
}

func TestFollowerPointsClientToLeader(t *testing.T) {
	n := testNode(t)
	if r := post(n.Handler(), `{"command":"get x"}`); r.Code != 409 {
		t.Fatalf("no leader known: %d", r.Code)
	}
	if _, err := n.HandleAppendEntries(AppendEntriesRequest{Term: 1, LeaderID: "n2"}); err != nil {
		t.Fatal(err)
	}
	r := post(n.Handler(), `{"command":"get x"}`)
	if r.Code != 409 || !strings.Contains(r.Body.String(), `"leader_address":"http://n2"`) {
		t.Fatalf("redirect: %d %s", r.Code, r.Body)
	}
	if len(n.Log().Entries) != 0 {
		t.Fatal("follower accepted a client command")
	}
}

// Клиентской части нужен только Propose с каналами: проверяем её без узла Raft.
type fakeProposer struct{ ch chan ApplyResult }

func (f fakeProposer) Propose(commands []string) ([]<-chan ApplyResult, error) {
	return []<-chan ApplyResult{f.ch}, nil
}

func TestClientWaitsOnChannel(t *testing.T) {
	done := fakeProposer{ch: make(chan ApplyResult, 1)}
	done.ch <- ApplyResult{Index: 7, Term: 2, Result: Result{Value: "v", Found: true}}
	if r := post(handleAddEntries(done), `{"command":"get k"}`); r.Code != 200 || !strings.Contains(r.Body.String(), `"index":7`) {
		t.Fatalf("result: %d %s", r.Code, r.Body)
	}
	lost := fakeProposer{ch: make(chan ApplyResult, 1)}
	lost.ch <- ApplyResult{Index: 7, Err: ErrEntryLost}
	if r := post(handleAddEntries(lost), `{"command":"get k"}`); r.Code != 503 {
		t.Fatalf("lost: %d", r.Code)
	}
	// Никто не ответит: клиент прекращает ждать по таймауту запроса.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "/log", strings.NewReader(`{"command":"get k"}`)).WithContext(ctx)
	recorder := httptest.NewRecorder()
	handleAddEntries(fakeProposer{ch: make(chan ApplyResult)}).ServeHTTP(recorder, request)
	if recorder.Code != 504 {
		t.Fatalf("timeout: %d", recorder.Code)
	}
}
