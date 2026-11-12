package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Role — локальная роль узла: последователь, кандидат или лидер.
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Config struct {
	ID          string
	Peers       map[string]string // ID остальных узлов → HTTP-адреса. Самого себя здесь нет.
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
}

// Entry — одна запись журнала. Индексы начинаются с 1, Term — срок лидера,
// который создал запись. Command — команда машины состояний, например "set a 1".
type Entry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command string `json:"command"`
}

// Поля RequestVote — как в Figure 2 статьи о Raft.
// LastLogIndex и LastLogTerm нужны, чтобы не выбрать лидера с отставшим журналом.
type VoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntries реплицирует записи журнала. С пустым Entries это heartbeat.
// PrevLogIndex/PrevLogTerm — запись перед новыми: по ней follower проверяет,
// что его журнал совпадает с журналом лидера до этого места.
type AppendEntriesRequest struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// ConflictTerm/ConflictIndex — подсказка при отказе (оптимизация из конца §5.3):
// по ней лидер перескакивает через весь расходящийся срок, а не идёт по одной записи.
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

// Transport отделяет правила Raft от конкретного способа передачи сообщений.
type Transport interface {
	RequestVote(context.Context, string, VoteRequest) (VoteResponse, error)
	AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error)
}

type Status struct {
	ID           string            `json:"id"`
	Role         Role              `json:"role"`
	Term         uint64            `json:"term"`
	LeaderID     string            `json:"leader_id"`
	VotedFor     string            `json:"voted_for"`
	Votes        int               `json:"votes"`
	ClusterSize  int               `json:"cluster_size"`
	Majority     int               `json:"majority"`
	LastLogIndex uint64            `json:"last_log_index"`
	LastLogTerm  uint64            `json:"last_log_term"`
	CommitIndex  uint64            `json:"commit_index"`
	LastApplied  uint64            `json:"last_applied"`
	NextIndex    map[string]uint64 `json:"next_index,omitempty"`  // Только у лидера.
	MatchIndex   map[string]uint64 `json:"match_index,omitempty"` // Только у лидера.
	Peers        map[string]string `json:"peers"`
}

// В Figure 2 это постоянное состояние: currentTerm, votedFor и log[].
// По условию задания узел хранит его только в памяти. После перезапуска процесс
// начинает с пустого журнала и срока 0 и догоняет лидера как новый узел.
type PersistentState struct {
	Term     uint64
	VotedFor string
	Log      []Entry
}

// Node хранит общее состояние для цикла Raft и входящих HTTP-обработчиков.
// Mutex защищает изменяемые поля. Сетевые вызовы выполняются без него.
// Суффикс Locked у метода означает: вызывающий код уже должен держать mutex.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	transport Transport
	logger    *slog.Logger
	state     PersistentState // currentTerm, votedFor и журнал.
	role      Role
	leaderID  string
	votes     int
	// Изменчивое состояние любого узла.
	commitIndex uint64 // Наибольший индекс, про который известно, что он зафиксирован.
	lastApplied uint64 // Наибольший индекс, применённый к машине состояний.
	// Изменчивое состояние лидера: заново заполняется после каждых выборов.
	nextIndex  map[string]uint64 // Какую запись отправить каждому peer следующей.
	matchIndex map[string]uint64 // До какого индекса журнал peer точно совпадает с нашим.
	kv         KV                // Машина состояний: key-value в памяти.
	// Клиенты, ждущие выполнения своих записей: индекс → канал для результата.
	waiters       map[uint64]waiter
	deadline      time.Time // Когда начать выборы, если не придёт актуальный AppendEntries.
	nextHeartbeat time.Time // Когда лидеру отправить следующий AppendEntries.
	running       bool
}

// New создаёт узел в роли follower с пустым журналом.
func New(cfg Config, transport Transport, logger *slog.Logger) (*Node, error) {
	if cfg.ID == "" || transport == nil {
		return nil, errors.New("id and transport are required")
	}
	if cfg.Heartbeat <= 0 || cfg.ElectionMin <= 2*cfg.Heartbeat || cfg.ElectionMax <= cfg.ElectionMin {
		return nil, errors.New("require 0 < 2*heartbeat < election-min < election-max")
	}
	peers := make(map[string]string, len(cfg.Peers))
	for id, address := range cfg.Peers {
		if id == "" || id == cfg.ID || address == "" {
			return nil, errors.New("peers must have unique nonempty IDs excluding self, and addresses")
		}
		peers[id] = address
	}
	cfg.Peers = peers
	if logger == nil {
		logger = slog.Default()
	}
	n := &Node{cfg: cfg, transport: transport, logger: logger, role: Follower,
		kv: NewKV(), waiters: make(map[uint64]waiter)}
	n.resetDeadlineLocked()
	return n, nil
}

// Большинство считается от всего фиксированного состава, включая недоступные узлы.
func (n *Node) majority() int { return (len(n.cfg.Peers)+1)/2 + 1 }

// Индекс последней записи журнала; 0 — журнал пуст.
func (n *Node) lastLogIndex() uint64 { return uint64(len(n.state.Log)) }

// Срок записи с данным индексом. Для индекса 0 и несуществующих записей — 0.
func (n *Node) termAt(index uint64) uint64 {
	if index == 0 || index > n.lastLogIndex() {
		return 0
	}
	return n.state.Log[index-1].Term
}

func (n *Node) lastLogTerm() uint64 { return n.termAt(n.lastLogIndex()) }

// Случайный таймаут уменьшает вероятность одновременного начала выборов.
func (n *Node) resetDeadlineLocked() {
	n.deadline = time.Now().Add(n.cfg.ElectionMin + time.Duration(rand.Int64N(int64(n.cfg.ElectionMax-n.cfg.ElectionMin))))
}

// Переход в follower. Состояние лидера (nextIndex, matchIndex) больше не нужно.
func (n *Node) becomeFollowerLocked(leaderID string) {
	n.role, n.leaderID, n.votes = Follower, leaderID, 0
	n.nextIndex, n.matchIndex = nil, nil
}

// Больший срок переводит любой узел в follower и очищает голос старого срока.
// Журнал при этом сохраняется: меняются только currentTerm и votedFor.
func (n *Node) observeTermLocked(term uint64) {
	if term <= n.state.Term {
		return
	}
	n.state.Term, n.state.VotedFor = term, ""
	n.becomeFollowerLocked("")
	n.resetDeadlineLocked()
}

func (n *Node) member(id string) bool {
	if id == n.cfg.ID {
		return true
	}
	_, ok := n.cfg.Peers[id]
	return ok
}

func copyMap[V any](m map[string]V) map[string]V {
	if m == nil {
		return nil
	}
	c := make(map[string]V, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// Status возвращает согласованный снимок под mutex; таблицы копируются.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.cfg.ID, Role: n.role, Term: n.state.Term, LeaderID: n.leaderID,
		VotedFor: n.state.VotedFor, Votes: n.votes, ClusterSize: len(n.cfg.Peers) + 1, Majority: n.majority(),
		LastLogIndex: n.lastLogIndex(), LastLogTerm: n.lastLogTerm(), CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		NextIndex: copyMap(n.nextIndex), MatchIndex: copyMap(n.matchIndex), Peers: copyMap(n.cfg.Peers)}
}
