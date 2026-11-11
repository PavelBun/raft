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

type VoteRequest struct {
	Term        uint64 `json:"term"`
	CandidateID string `json:"candidate_id"`
}
type VoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// На первом этапе AppendEntries не содержит записей журнала, только heartbeat.
type HeartbeatRequest struct {
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
}
type HeartbeatResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

// Transport отделяет правила Raft от конкретного способа передачи сообщений.
type Transport interface {
	RequestVote(context.Context, string, VoteRequest) (VoteResponse, error)
	Heartbeat(context.Context, string, HeartbeatRequest) (HeartbeatResponse, error)
}

type Status struct {
	ID          string            `json:"id"`
	Role        Role              `json:"role"`
	Term        uint64            `json:"term"`
	LeaderID    string            `json:"leader_id"`
	VotedFor    string            `json:"voted_for"`
	Votes       int               `json:"votes"`
	ClusterSize int               `json:"cluster_size"`
	Majority    int               `json:"majority"`
	Peers       map[string]string `json:"peers"`
	Error       string            `json:"error,omitempty"`
}

// Node хранит общее состояние для цикла Raft и входящих HTTP-обработчиков.
// Mutex защищает изменяемые поля. Сетевые вызовы выполняются без него.
// Суффикс Locked у метода означает: вызывающий код уже должен держать mutex.
type Node struct {
	mu            sync.Mutex
	cfg           Config
	storage       Storage
	transport     Transport
	logger        *slog.Logger
	state         PersistentState // Срок и голос: дополнительно сохраняются на диск.
	role          Role
	leaderID      string
	votes         int
	deadline      time.Time // Когда начать выборы, если не придёт актуальный heartbeat.
	nextHeartbeat time.Time // Когда лидеру отправить следующую порцию heartbeat.
	failure       error
	running       bool
}

// New загружает срок и голос с диска и создаёт узел в роли follower.
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
	if logger == nil {
		logger = slog.Default()
	}
	n := &Node{cfg: cfg, storage: storage, transport: transport, logger: logger, state: state, role: Follower}
	n.resetDeadlineLocked()
	return n, nil
}

// Большинство считается от всего фиксированного состава, включая недоступные узлы.
func (n *Node) majority() int { return (len(n.cfg.Peers)+1)/2 + 1 }

// Случайный таймаут уменьшает вероятность одновременного начала выборов.
func (n *Node) resetDeadlineLocked() {
	n.deadline = time.Now().Add(n.cfg.ElectionMin + time.Duration(rand.Int64N(int64(n.cfg.ElectionMax-n.cfg.ElectionMin))))
}

// При ошибке диска прекращаем участие: нельзя подтверждать несохранённый голос.
func (n *Node) persistLocked() error {
	if err := n.storage.Save(n.state); err != nil {
		n.failure = fmt.Errorf("persist state: %w", err)
		n.role, n.leaderID = Follower, ""
		return n.failure
	}
	return nil
}

// Больший срок переводит любой узел в follower и очищает голос старого срока.
func (n *Node) observeTermLocked(term uint64) error {
	if term <= n.state.Term {
		return nil
	}
	n.state = PersistentState{Term: term}
	n.role, n.leaderID, n.votes = Follower, "", 0
	n.resetDeadlineLocked()
	return n.persistLocked()
}

func (n *Node) member(id string) bool {
	if id == n.cfg.ID {
		return true
	}
	_, ok := n.cfg.Peers[id]
	return ok
}

// Status возвращает согласованный снимок под mutex, включая копию таблицы peers.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	peers := make(map[string]string, len(n.cfg.Peers))
	for id, address := range n.cfg.Peers {
		peers[id] = address
	}
	s := Status{ID: n.cfg.ID, Role: n.role, Term: n.state.Term, LeaderID: n.leaderID,
		VotedFor: n.state.VotedFor, Votes: n.votes, ClusterSize: len(peers) + 1, Majority: n.majority(), Peers: peers}
	if n.failure != nil {
		s.Error = n.failure.Error()
	}
	return s
}
