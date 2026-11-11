package raft

// Запросы клиента (§5.3, §8): добавить команды в журнал и посмотреть журнал.
// Команды принимает только лидер. Ответ клиенту уходит после фиксации записи.

import (
	"context"
	"errors"
	"time"
)

// ErrEntryLost: запись не зафиксирована, и новый лидер заменил её другой записью.
// Такую запись клиент может отправить заново.
var ErrEntryLost = errors.New("entry was replaced by a new leader")

// NotLeaderError говорит клиенту, куда обратиться. Лидер может быть неизвестен.
type NotLeaderError struct {
	LeaderID      string
	LeaderAddress string
}

func (e *NotLeaderError) Error() string { return "not the leader" }

// Propose дописывает команды в журнал лидера и сохраняет его на диск.
// Возвращает индекс последней новой записи и срок: по ним ждёт WaitCommitted.
func (n *Node) Propose(commands []string) (index, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failure != nil {
		return 0, 0, n.failure
	}
	if n.role != Leader {
		return 0, 0, &NotLeaderError{LeaderID: n.leaderID, LeaderAddress: n.cfg.Peers[n.leaderID]}
	}
	for _, command := range commands {
		n.state.Log = append(n.state.Log, Entry{Index: n.lastLogIndex() + 1, Term: n.state.Term, Command: command})
	}
	// Лидер считает себя репликой записи, поэтому сначала она должна попасть на диск.
	if err := n.persistLocked(); err != nil {
		return 0, 0, err
	}
	n.nextHeartbeat = time.Time{} // Разослать на ближайшем тике, не дожидаясь heartbeat.
	n.advanceCommitLocked()       // Кластер из одного узла фиксирует запись сразу.
	return n.lastLogIndex(), n.state.Term, nil
}

// WaitCommitted ждёт, пока запись с данным индексом и сроком будет зафиксирована.
// Пара (индекс, срок) однозначно определяет запись. Если индекс занят записью
// другого срока или пропал, наша запись потеряна. Смена лидера сама по себе этого
// не значит: новый лидер может зафиксировать запись, поэтому ждём до отмены ctx.
func (n *Node) WaitCommitted(ctx context.Context, index, term uint64) error {
	for {
		n.mu.Lock()
		if n.termAt(index) != term {
			n.mu.Unlock()
			return ErrEntryLost
		}
		if n.commitIndex >= index {
			n.mu.Unlock()
			return nil
		}
		changed := n.changed
		n.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// LogEntryView — запись журнала с отметкой о фиксации для GET /log.
type LogEntryView struct {
	Entry
	Committed bool `json:"committed"`
}

// LogView — снимок журнала узла. На разных узлах незафиксированный хвост может отличаться.
type LogView struct {
	ID          string         `json:"id"`
	Role        Role           `json:"role"`
	Term        uint64         `json:"term"`
	LeaderID    string         `json:"leader_id"`
	CommitIndex uint64         `json:"commit_index"`
	LastApplied uint64         `json:"last_applied"`
	Entries     []LogEntryView `json:"entries"`
}

// Log возвращает согласованную копию журнала под mutex.
func (n *Node) Log() LogView {
	n.mu.Lock()
	defer n.mu.Unlock()
	entries := make([]LogEntryView, len(n.state.Log))
	for i, e := range n.state.Log {
		entries[i] = LogEntryView{Entry: e, Committed: e.Index <= n.commitIndex}
	}
	return LogView{ID: n.cfg.ID, Role: n.role, Term: n.state.Term, LeaderID: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied, Entries: entries}
}
