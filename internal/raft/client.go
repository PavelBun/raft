package raft

// Запросы клиента (§5.3, §8): добавить команды в журнал и посмотреть журнал.
// Команды принимает только лидер. Клиент получает результат машины состояний
// после того, как запись зафиксирована и выполнена.
//
// Разделение обязанностей: Propose — часть Raft, работает под mutex узла.
// Он дописывает записи и выдаёт клиенту канал на каждую. Дальше клиентский код
// ничего не знает о Raft и не берёт mutex: он только ждёт результат на канале.
// Результат в канал отправляет applyCommittedLocked при выполнении записи.

import (
	"errors"
	"time"
)

// ErrEntryLost: запись не зафиксирована, и новый лидер заменил её другой записью.
// Такую команду клиент может отправить заново.
var ErrEntryLost = errors.New("entry was replaced by a new leader")

// NotLeaderError говорит клиенту, куда обратиться. Лидер может быть неизвестен.
type NotLeaderError struct {
	LeaderID      string
	LeaderAddress string
}

func (e *NotLeaderError) Error() string { return "not the leader" }

// ApplyResult — то, что клиент получает из канала: результат команды
// или ошибку, если запись была заменена.
type ApplyResult struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Result
	Err error `json:"-"`
}

// waiter — ожидание одной записи. Пара (индекс, срок) однозначно определяет запись:
// если на этом индексе выполнится запись другого срока, наша потеряна.
type waiter struct {
	term uint64
	ch   chan ApplyResult
}

// Propose дописывает команды в журнал лидера и возвращает по каналу на каждую.
// В канал придёт ровно один результат. Канал буферизован, поэтому применение
// записи не блокируется, даже если клиент уже перестал ждать.
func (n *Node) Propose(commands []string) ([]<-chan ApplyResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return nil, &NotLeaderError{LeaderID: n.leaderID, LeaderAddress: n.cfg.Peers[n.leaderID]}
	}
	results := make([]<-chan ApplyResult, len(commands))
	for i, command := range commands {
		e := Entry{Index: n.lastLogIndex() + 1, Term: n.state.Term, Command: command}
		n.state.Log = append(n.state.Log, e)
		ch := make(chan ApplyResult, 1)
		n.waiters[e.Index] = waiter{term: e.Term, ch: ch}
		results[i] = ch
	}
	n.nextHeartbeat = time.Time{} // Разослать на ближайшем тике, не дожидаясь heartbeat.
	n.advanceCommitLocked()       // Кластер из одного узла фиксирует запись сразу.
	return results, nil
}

// Сообщаем ожидающим клиентам о результате выполнения записи.
func (n *Node) notifyWaiterLocked(e Entry, result Result) {
	w, ok := n.waiters[e.Index]
	if !ok {
		return
	}
	delete(n.waiters, e.Index)
	// Страховка: обычно о заменённой записи клиент узнаёт раньше, при обрезке
	// журнала (dropWaitersFromLocked). Чужую запись нельзя выдать за его результат.
	if w.term != e.Term {
		w.ch <- ApplyResult{Index: e.Index, Term: w.term, Err: ErrEntryLost}
		return
	}
	w.ch <- ApplyResult{Index: e.Index, Term: e.Term, Result: result}
}

// Журнал обрезан начиная с from: записи клиентов на этих индексах потеряны.
func (n *Node) dropWaitersFromLocked(from uint64) {
	for index, w := range n.waiters {
		if index >= from {
			delete(n.waiters, index)
			w.ch <- ApplyResult{Index: index, Term: w.term, Err: ErrEntryLost}
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

// KVView — состояние машины состояний этого узла для GET /kv.
type KVView struct {
	ID          string            `json:"id"`
	LastApplied uint64            `json:"last_applied"`
	Data        map[string]string `json:"data"`
}

func (n *Node) KV() KVView {
	n.mu.Lock()
	defer n.mu.Unlock()
	return KVView{ID: n.cfg.ID, LastApplied: n.lastApplied, Data: n.kv.Snapshot()}
}
