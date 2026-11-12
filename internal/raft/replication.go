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

// Предел размера записей в одном AppendEntries. Отставший follower догоняет
// лидера пачками, по одной на heartbeat. Пачка считается по байтам, а не по
// числу записей: коротких команд уходят тысячи за раз, длинных — меньше.
const maxBatchBytes = 512 << 10

// Оценка размера записи в JSON сверху: служебные поля плюс команда с запасом
// на экранирование (символ может превратиться в \u00XX — до 6 байт).
func entrySize(e Entry) int { return 64 + 6*len(e.Command) }

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
	if !n.member(req.LeaderID) {
		return AppendEntriesResponse{}, errors.New("unknown leader")
	}
	n.observeTermLocked(req.Term)
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
	// Иначе отвечаем false с подсказкой, откуда лидеру пробовать со следующим heartbeat.
	if req.PrevLogIndex > n.lastLogIndex() || n.termAt(req.PrevLogIndex) != req.PrevLogTerm {
		res.ConflictTerm, res.ConflictIndex = n.conflictHintLocked(req.PrevLogIndex)
		return res, nil
	}
	// 3. Конфликт (тот же индекс, другой срок) — удаляем эту запись и все после неё.
	// 4. Дописываем записи, которых ещё нет. Уже имеющиеся совпадающие записи
	// не трогаем: запоздавший старый запрос не должен обрезать журнал.
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
			n.dropWaitersFromLocked(e.Index)
		}
		n.state.Log = append(n.state.Log, req.Entries[i:]...)
		break
	}
	// 5. commitIndex = min(leaderCommit, индекс последней новой записи).
	// Дальше последней новой записи нельзя: остальная часть журнала ещё не проверена.
	if req.LeaderCommit > n.commitIndex {
		n.setCommitIndexLocked(min(req.LeaderCommit, req.PrevLogIndex+uint64(len(req.Entries))))
	}
	res.Success = true
	return res, nil
}

// Подсказка лидеру при отказе на PrevLogIndex = prev:
//   - журнал короче prev: ConflictTerm = 0, ConflictIndex = длина + 1 —
//     всё, что дальше нашего журнала, проверять бессмысленно;
//   - на prev запись другого срока: ConflictTerm = этот срок, ConflictIndex = первая
//     запись этого срока у нас. Лидер перескакивает сразу в начало этого срока.
//     Если часть записей этого срока всё же совпадает с лидером, лидер пришлёт их
//     ещё раз, и мы их просто пропустим: это безопасно, только чуть больше данных.
func (n *Node) conflictHintLocked(prev uint64) (term, index uint64) {
	if prev > n.lastLogIndex() {
		return 0, n.lastLogIndex() + 1
	}
	term = n.termAt(prev)
	index = prev
	for index > 1 && n.termAt(index-1) == term {
		index--
	}
	return term, index
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

// replicateTo отправляет одному peer один AppendEntries и обрабатывает ответ.
// Восстановление журнала ленивое: после отказа лидер только уменьшает nextIndex,
// а новая попытка уйдёт со следующим heartbeat. Оставшиеся записи тоже уйдут
// со следующим heartbeat. Сразу повторно запрос не отправляется.
func (n *Node) replicateTo(ctx context.Context, id, address string, term uint64) {
	n.mu.Lock()
	req, ok := n.appendEntriesRequestLocked(id, term)
	n.mu.Unlock()
	if !ok {
		return
	}
	// Пока ждём сеть, mutex свободен.
	res, err := n.transport.AppendEntries(ctx, address, req)
	if err != nil {
		return // Нет ответа — повторим на следующем heartbeat.
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handleAppendEntriesResponseLocked(id, req, res)
}

// Запрос для peer строится от nextIndex[id]: PrevLogIndex — запись перед ним.
// В пачку берём записи, пока их оценочный размер не превысит maxBatchBytes;
// одна запись помещается всегда, потому что команда не длиннее maxCommandBytes.
// Записи копируются, потому что после снятия mutex журнал может измениться.
func (n *Node) appendEntriesRequestLocked(id string, term uint64) (AppendEntriesRequest, bool) {
	if n.role != Leader || n.state.Term != term {
		return AppendEntriesRequest{}, false
	}
	prev := n.nextIndex[id] - 1
	end, size := prev, 0
	for end < n.lastLogIndex() && size+entrySize(n.state.Log[end]) <= maxBatchBytes {
		size += entrySize(n.state.Log[end])
		end++
	}
	entries := make([]Entry, end-prev)
	copy(entries, n.state.Log[prev:end])
	return AppendEntriesRequest{Term: term, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: n.termAt(prev),
		Entries: entries, LeaderCommit: n.commitIndex}, true
}

// Обработка ответа peer: обновить nextIndex/matchIndex или уступить лидерство.
func (n *Node) handleAppendEntriesResponseLocked(id string, req AppendEntriesRequest, res AppendEntriesResponse) {
	// Больший срок в ответе: мы устаревший лидер, становимся follower.
	n.observeTermLocked(res.Term)
	// Запоздалый ответ из прошлого срока ничего не значит.
	if n.role != Leader || n.state.Term != req.Term || res.Term != req.Term {
		return
	}
	if res.Success {
		// Журнал peer совпадает с нашим до последней отправленной записи.
		// Ответы могут прийти не по порядку, поэтому значения только растут.
		match := req.PrevLogIndex + uint64(len(req.Entries))
		n.matchIndex[id] = max(n.matchIndex[id], match)
		n.nextIndex[id] = max(n.nextIndex[id], match+1)
		n.advanceCommitLocked()
		return
	}
	// Журналы не совпали на PrevLogIndex: переходим на ConflictIndex из подсказки
	// follower, а не уменьшаем nextIndex на один. Повторная попытка — со следующим
	// heartbeat. Подсказка всегда не больше PrevLogIndex; странную подсказку
	// заменяем обычным шагом на одну запись. Первое условие защищает от повторного
	// отката по двум ответам на одинаковые запросы.
	if n.nextIndex[id] == req.PrevLogIndex+1 && n.nextIndex[id] > 1 {
		next := res.ConflictIndex
		if next < 1 || next > req.PrevLogIndex {
			next = req.PrevLogIndex
		}
		n.nextIndex[id] = next
	}
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
}

// Применение к машине состояний: lastApplied догоняет commitIndex строго по порядку.
// Результат команды получает клиент, если он ждёт эту запись на этом узле.
func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.state.Log[n.lastApplied-1]
		result := n.kv.Apply(e.Command)
		n.logger.Info("applied entry", "id", n.cfg.ID, "index", e.Index, "term", e.Term, "command", e.Command, "found", result.Found, "value", result.Value)
		n.notifyWaiterLocked(e, result)
	}
}
