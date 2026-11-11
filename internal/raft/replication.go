package raft

// Репликация журнала (§5.3 и §5.4 статьи о Raft, Figure 2).
// Лидер рассылает AppendEntries: с записями или пустой как heartbeat.
// Follower проверяет совпадение журналов, дописывает записи и узнаёт commitIndex.
// Лидер считает запись зафиксированной, когда она есть у большинства.

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Сколько записей лидер отправляет в одном AppendEntries.
// Отставший follower догоняет лидера несколькими запросами подряд.
const maxEntriesPerRPC = 64

// validate проверяет форму запроса до любых изменений состояния:
// записи должны идти подряд сразу после PrevLogIndex, а их сроки — не убывать
// и не превышать срок лидера.
func (r AppendEntriesRequest) validate() error {
	if r.Term == 0 || r.PrevLogTerm > r.Term || (r.PrevLogIndex == 0) != (r.PrevLogTerm == 0) {
		return errors.New("positive term and consistent prev_log_index/prev_log_term required")
	}
	prevTerm := r.PrevLogTerm
	for i, e := range r.Entries {
		if e.Index != r.PrevLogIndex+uint64(i)+1 || e.Term < prevTerm || e.Term > r.Term {
			return errors.New("entries must be consecutive after prev_log_index with nondecreasing terms")
		}
		prevTerm = e.Term
	}
	return nil
}

// HandleAppendEntries — получатель AppendEntries, пункты 1–5 из Figure 2.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if err := req.validate(); err != nil {
		return AppendEntriesResponse{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failure != nil {
		return AppendEntriesResponse{}, n.failure
	}
	if !n.member(req.LeaderID) {
		return AppendEntriesResponse{}, errors.New("unknown leader")
	}
	if err := n.observeTermLocked(req.Term); err != nil {
		return AppendEntriesResponse{}, err
	}
	res := AppendEntriesResponse{Term: n.state.Term}
	// 1. Запрос от лидера устаревшего срока отклоняем, таймер не трогаем.
	if req.Term < n.state.Term {
		return res, nil
	}
	// Это законный лидер текущего срока: признаём его и откладываем выборы,
	// даже если журналы ниже не совпадут. Голос этого срока не очищаем.
	n.becomeFollowerLocked(req.LeaderID)
	n.resetDeadlineLocked()
	// 2. В журнале должна быть запись PrevLogIndex со сроком PrevLogTerm.
	// Иначе отвечаем false, и лидер повторит запрос с записи пораньше.
	if req.PrevLogIndex > n.lastLogIndex() || n.termAt(req.PrevLogIndex) != req.PrevLogTerm {
		return res, nil
	}
	// 3. Конфликт (тот же индекс, другой срок) — удаляем эту запись и все после неё.
	// 4. Дописываем записи, которых ещё нет. Уже имеющиеся совпадающие записи
	// не трогаем: запоздавший старый запрос не должен обрезать журнал.
	changed := false
	for i, e := range req.Entries {
		if e.Index <= n.lastLogIndex() {
			if n.termAt(e.Index) == e.Term {
				continue
			}
			if e.Index <= n.commitIndex {
				// По доказательству безопасности Raft такого быть не может.
				return AppendEntriesResponse{}, fmt.Errorf("refusing to overwrite committed entry %d", e.Index)
			}
			n.state.Log = n.state.Log[:e.Index-1]
			n.notifyLocked()
		}
		n.state.Log = append(n.state.Log, req.Entries[i:]...)
		changed = true
		break
	}
	// Записи сохраняются на диск до ответа success: лидер посчитает их реплицированными.
	if changed {
		if err := n.persistLocked(); err != nil {
			return AppendEntriesResponse{}, err
		}
	}
	// 5. commitIndex = min(leaderCommit, индекс последней новой записи).
	// Дальше последней новой записи нельзя: остальная часть журнала ещё не проверена.
	if req.LeaderCommit > n.commitIndex {
		n.setCommitIndexLocked(min(req.LeaderCommit, req.PrevLogIndex+uint64(len(req.Entries))))
	}
	res.Success = true
	return res, nil
}

// Рассылка AppendEntries: отдельная goroutine на каждого peer, включая недоступных.
// Медленный или отставший peer не задерживает остальных.
func (n *Node) broadcastAppendEntries(ctx context.Context, workers *sync.WaitGroup, term uint64) {
	for id, address := range n.cfg.Peers {
		workers.Add(1)
		go func(id, address string) {
			defer workers.Done()
			n.replicateTo(ctx, id, address, term)
		}(id, address)
	}
}

// replicateTo доводит журнал одного peer до журнала лидера: отправляет запрос,
// а при отказе или если остались записи — сразу следующий. Нет ответа —
// выходим: повторим на следующем heartbeat.
func (n *Node) replicateTo(ctx context.Context, id, address string, term uint64) {
	for {
		n.mu.Lock()
		req, ok := n.appendEntriesRequestLocked(id, term)
		n.mu.Unlock()
		if !ok {
			return
		}
		// Пока ждём сеть, mutex свободен.
		res, err := n.transport.AppendEntries(ctx, address, req)
		if err != nil {
			return
		}
		n.mu.Lock()
		again := n.handleAppendEntriesResponseLocked(id, req, res)
		n.mu.Unlock()
		if !again {
			return
		}
	}
}

// Запрос для peer строится от nextIndex[id]: PrevLogIndex — запись перед ним.
// Записи копируются, потому что после снятия mutex журнал может измениться.
func (n *Node) appendEntriesRequestLocked(id string, term uint64) (AppendEntriesRequest, bool) {
	if n.failure != nil || n.role != Leader || n.state.Term != term {
		return AppendEntriesRequest{}, false
	}
	prev := n.nextIndex[id] - 1
	end := min(n.lastLogIndex(), prev+maxEntriesPerRPC)
	entries := make([]Entry, end-prev)
	copy(entries, n.state.Log[prev:end])
	return AppendEntriesRequest{Term: term, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: n.termAt(prev),
		Entries: entries, LeaderCommit: n.commitIndex}, true
}

// Обработка ответа peer. Возвращает true, если нужно сразу отправить ему ещё один запрос.
func (n *Node) handleAppendEntriesResponseLocked(id string, req AppendEntriesRequest, res AppendEntriesResponse) bool {
	if n.failure != nil {
		return false
	}
	// Больший срок в ответе: мы устаревший лидер, становимся follower.
	if err := n.observeTermLocked(res.Term); err != nil {
		return false
	}
	// Запоздалый ответ из прошлого срока ничего не значит.
	if n.role != Leader || n.state.Term != req.Term || res.Term != req.Term {
		return false
	}
	if res.Success {
		// Журнал peer совпадает с нашим до последней отправленной записи.
		// Ответы могут прийти не по порядку, поэтому значения только растут.
		match := req.PrevLogIndex + uint64(len(req.Entries))
		n.matchIndex[id] = max(n.matchIndex[id], match)
		n.nextIndex[id] = max(n.nextIndex[id], match+1)
		n.advanceCommitLocked()
		return n.nextIndex[id] <= n.lastLogIndex()
	}
	// Журналы не совпали на PrevLogIndex: уменьшаем nextIndex на один и повторяем.
	// Условие защищает от двойного уменьшения по двум ответам на одинаковые запросы.
	if n.nextIndex[id] == req.PrevLogIndex+1 && n.nextIndex[id] > 1 {
		n.nextIndex[id]--
		return true
	}
	return false
}

// Правило лидера из Figure 2: если есть N > commitIndex, такое что
// matchIndex[i] ≥ N у большинства и log[N].term == currentTerm, то commitIndex = N.
// Записи прошлых сроков подсчётом реплик не фиксируются (§5.4.2, Figure 8):
// они станут зафиксированными вместе с первой записью текущего срока.
func (n *Node) advanceCommitLocked() {
	for index := n.lastLogIndex(); index > n.commitIndex && n.termAt(index) == n.state.Term; index-- {
		replicas := 1 // Сам лидер.
		for _, match := range n.matchIndex {
			if match >= index {
				replicas++
			}
		}
		if replicas >= n.majority() {
			n.setCommitIndexLocked(index)
			return
		}
	}
}

// commitIndex только растёт. После фиксации применяем записи по порядку.
func (n *Node) setCommitIndexLocked(index uint64) {
	if index <= n.commitIndex {
		return
	}
	n.commitIndex = index
	n.applyCommittedLocked()
	n.notifyLocked()
}

// Применение к машине состояний: lastApplied догоняет commitIndex строго по порядку.
// Самой машины состояний (key-value) пока нет, поэтому запись только логируется.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.state.Log[n.lastApplied-1]
		n.logger.Info("applied entry", "id", n.cfg.ID, "index", e.Index, "term", e.Term, "command", e.Command)
	}
}
