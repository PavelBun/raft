package raft

// Выборы лидера (§5.2 и §5.4.1 статьи о Raft): обработка чужого запроса голоса
// (HandleVote), начало собственной кампании, рассылка RequestVote и подсчёт голосов.

import (
	"context"
	"errors"
	"sync"
	"time"
)

// HandleVote обрабатывает входящий запрос: другой кандидат просит у нас голос.
func (n *Node) HandleVote(req VoteRequest) (VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failure != nil {
		return VoteResponse{}, n.failure
	}
	if !n.member(req.CandidateID) {
		return VoteResponse{}, errors.New("unknown candidate")
	}
	if err := n.observeTermLocked(req.Term); err != nil {
		return VoteResponse{}, err
	}
	res := VoteResponse{Term: n.state.Term}
	if req.Term < n.state.Term {
		return res, nil
	}
	// Голосуем, только если ещё не голосовали в этом сроке (или голосовали за него же)
	// и журнал кандидата не хуже нашего. Иначе лидером мог бы стать узел,
	// у которого нет уже зафиксированных записей (§5.4.1).
	if (n.state.VotedFor == "" || n.state.VotedFor == req.CandidateID) && n.candidateLogUpToDateLocked(req) {
		n.state.VotedFor = req.CandidateID
		// Сначала сохраняем голос, только потом отвечаем vote_granted=true.
		if err := n.persistLocked(); err != nil {
			return VoteResponse{}, err
		}
		n.resetDeadlineLocked()
		res.VoteGranted = true
	}
	return res, nil
}

// Журнал кандидата «не хуже», если его последняя запись из более позднего срока,
// а при равных сроках — если его журнал не короче нашего.
func (n *Node) candidateLogUpToDateLocked(req VoteRequest) bool {
	if req.LastLogTerm != n.lastLogTerm() {
		return req.LastLogTerm > n.lastLogTerm()
	}
	return req.LastLogIndex >= n.lastLogIndex()
}

// startElectionLocked начинает кампанию: новый срок, свой голос и новый таймаут.
// Состояние сохраняется до отправки первого запроса другим узлам.
func (n *Node) startElectionLocked() error {
	n.state.Term++
	n.state.VotedFor = n.cfg.ID
	n.role, n.leaderID, n.votes = Candidate, "", 1
	n.resetDeadlineLocked()
	if err := n.persistLocked(); err != nil {
		return err
	}
	n.logger.Info("started election", "id", n.cfg.ID, "term", n.state.Term)
	// Кластер из одного узла уже имеет большинство за счёт собственного голоса.
	if n.votes >= n.majority() {
		n.becomeLeaderLocked()
	}
	return nil
}

// Запрос голоса собирается под mutex сразу после начала кампании:
// срок и последняя запись журнала должны соответствовать именно ей.
func (n *Node) voteRequestLocked() VoteRequest {
	return VoteRequest{Term: n.state.Term, CandidateID: n.cfg.ID, LastLogIndex: n.lastLogIndex(), LastLogTerm: n.lastLogTerm()}
}

// Каждому peer отправляем отдельный запрос в своей goroutine, не ждём всех ответов.
// Ответ и голос обрабатывает requestVote; req.Term привязывает запрос к этой кампании.
func (n *Node) broadcastVoteRequests(ctx context.Context, workers *sync.WaitGroup, req VoteRequest) {
	for _, address := range n.cfg.Peers {
		workers.Add(1)
		go func(address string) {
			defer workers.Done()
			n.requestVote(ctx, address, req)
		}(address)
	}
}

// requestVote просит голос у одного peer и учитывает его ответ в общей кампании.
func (n *Node) requestVote(ctx context.Context, address string, req VoteRequest) {
	term := req.Term
	// Пока ждём сеть, mutex свободен: входящие сообщения продолжают обрабатываться.
	res, err := n.transport.RequestVote(ctx, address, req)
	if err != nil {
		return
	} // Нет ответа — нет голоса; это не доказательство отказа узла.
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failure != nil {
		return
	}
	if err := n.observeTermLocked(res.Term); err != nil {
		return
	}
	// Запоздалые ответы старой кампании нельзя учитывать в новом сроке.
	if n.role != Candidate || n.state.Term != term || res.Term != term {
		return
	}
	if res.VoteGranted {
		n.votes++ // Один запрос на peer в каждом сроке: голос не учитывается повторно.
		if n.votes >= n.majority() {
			n.becomeLeaderLocked()
		}
	}
}

// becomeLeaderLocked вызывается, когда кандидат набрал большинство голосов.
// Лидер пока ничего не знает о журналах peer: предполагает, что они совпадают
// с его журналом (nextIndex = последний+1), а подтверждённого совпадения нет (matchIndex = 0).
func (n *Node) becomeLeaderLocked() {
	n.role, n.leaderID = Leader, n.cfg.ID
	n.nextIndex = make(map[string]uint64, len(n.cfg.Peers))
	n.matchIndex = make(map[string]uint64, len(n.cfg.Peers))
	for id := range n.cfg.Peers {
		n.nextIndex[id] = n.lastLogIndex() + 1
		n.matchIndex[id] = 0
	}
	n.nextHeartbeat = time.Time{} // Первый AppendEntries будет отправлен на ближайшем тике.
	n.logger.Info("became leader", "id", n.cfg.ID, "term", n.state.Term, "votes", n.votes)
}
