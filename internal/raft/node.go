package raft

import (
	"context"
	"errors"
	"fmt"
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
// который создал запись. Command пока просто строка: смысл ей придаст
// машина состояний (key-value) на следующем этапе.
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
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
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
	Error        string            `json:"error,omitempty"`
}

// Node хранит общее состояние для цикла Raft и входящих HTTP-обработчиков.
// Mutex защищает изменяемые поля. Сетевые вызовы выполняются без него.
// Суффикс Locked у метода означает: вызывающий код уже должен держать mutex.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	storage   Storage
	transport Transport
	logger    *slog.Logger
	// Постоянное состояние из Figure 2: currentTerm, votedFor и log[].
	// Сохраняется на диск до ответа на RPC.
	state    PersistentState
	role     Role
	leaderID string
	votes    int
	// Изменчивое состояние любого узла: после перезапуска начинается с нуля.
	commitIndex uint64 // Наибольший индекс, про который известно, что он зафиксирован.
	lastApplied uint64 // Наибольший индекс, применённый к машине состояний.
	// Изменчивое состояние лидера: заново заполняется после каждых выборов.
	nextIndex  map[string]uint64 // Какую запись отправить каждому peer следующей.
	matchIndex map[string]uint64 // До какого индекса журнал peer точно совпадает с нашим.
	// changed закрывается при смене commitIndex, срока или журнала:
	// так ожидающий клиентский запрос узнаёт, что пора перепроверить запись.
	changed       chan struct{}
	deadline      time.Time // Когда начать выборы, если не придёт актуальный AppendEntries.
	nextHeartbeat time.Time // Когда лидеру отправить следующий AppendEntries.
	failure       error
	running       bool
}

// New загружает срок, голос и журнал с диска и создаёт узел в роли follower.
func New(cfg Config, storage Storage, transport Transport, logger *slog.Logger) (*Node, error) {
	if cfg.ID == "" || storage == nil || transport == nil {
		return nil, errors.New("id, storage and transport are required")
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
	state, err := storage.Load()
	if err != nil {
		return nil, err
	}
	if err := validateLog(state); err != nil {
		return nil, fmt.Errorf("stored state: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	n := &Node{cfg: cfg, storage: storage, transport: transport, logger: logger, state: state, role: Follower, changed: make(chan struct{})}
	n.resetDeadlineLocked()
	return n, nil
}

// Журнал с диска должен идти подряд с индекса 1, а сроки записей — не убывать
// и не превышать текущий срок узла.
func validateLog(state PersistentState) error {
	var prevTerm uint64
	for i, e := range state.Log {
		if e.Index != uint64(i)+1 || e.Term == 0 || e.Term < prevTerm || e.Term > state.Term {
			return fmt.Errorf("invalid log entry at position %d", i+1)
		}
		prevTerm = e.Term
	}
	return nil
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

// Будим всех, кто ждёт изменения состояния (клиентов POST /log).
func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

// Переход в follower. Состояние лидера (nextIndex, matchIndex) больше не нужно.
func (n *Node) becomeFollowerLocked(leaderID string) {
	n.role, n.leaderID, n.votes = Follower, leaderID, 0
	n.nextIndex, n.matchIndex = nil, nil
}

// При ошибке диска прекращаем участие: нельзя подтверждать несохранённые данные.
func (n *Node) persistLocked() error {
	if err := n.storage.Save(n.state); err != nil {
		n.failure = fmt.Errorf("persist state: %w", err)
		n.becomeFollowerLocked("")
		n.notifyLocked()
		return n.failure
	}
	return nil
}

// Больший срок переводит любой узел в follower и очищает голос старого срока.
// Журнал при этом сохраняется: меняются только currentTerm и votedFor.
func (n *Node) observeTermLocked(term uint64) error {
	if term <= n.state.Term {
		return nil
	}
	n.state.Term, n.state.VotedFor = term, ""
	n.becomeFollowerLocked("")
	n.resetDeadlineLocked()
	n.notifyLocked()
	return n.persistLocked()
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
	s := Status{ID: n.cfg.ID, Role: n.role, Term: n.state.Term, LeaderID: n.leaderID,
		VotedFor: n.state.VotedFor, Votes: n.votes, ClusterSize: len(n.cfg.Peers) + 1, Majority: n.majority(),
		LastLogIndex: n.lastLogIndex(), LastLogTerm: n.lastLogTerm(), CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		NextIndex: copyMap(n.nextIndex), MatchIndex: copyMap(n.matchIndex), Peers: copyMap(n.cfg.Peers)}
	if n.failure != nil {
		s.Error = n.failure.Error()
	}
	return s
}
