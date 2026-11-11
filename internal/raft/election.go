package raft

// Выборы лидера: обработка чужого запроса голоса (HandleVote),
// начало собственной кампании, рассылка RequestVote и подсчёт голосов.

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
	// Журналов пока нет. Проверка актуальности журнала кандидата появится на этапе 2.
	// Можно голосовать впервые или подтвердить голос тому же кандидату в этом сроке.
	if n.state.VotedFor == "" || n.state.VotedFor == req.CandidateID {
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

// Каждому peer отправляем отдельный запрос в своей goroutine, не ждём всех ответов.
// Ответ и голос обрабатывает requestVote; term привязывает запрос к этой кампании.
func (n *Node) broadcastVoteRequests(ctx context.Context, workers *sync.WaitGroup, term uint64) {
	for _, address := range n.cfg.Peers {
		workers.Add(1)
		go func(address string) {
			defer workers.Done()
			n.requestVote(ctx, address, term)
		}(address)
	}
}

// requestVote просит голос у одного peer и учитывает его ответ в общей кампании.
func (n *Node) requestVote(ctx context.Context, address string, term uint64) {
	// Пока ждём сеть, mutex свободен: входящие сообщения продолжают обрабатываться.
	res, err := n.transport.RequestVote(ctx, address, VoteRequest{Term: term, CandidateID: n.cfg.ID})
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
func (n *Node) becomeLeaderLocked() {
	n.role, n.leaderID = Leader, n.cfg.ID
	n.nextHeartbeat = time.Time{} // Первый heartbeat будет отправлен на ближайшем тике.
	n.logger.Info("became leader", "id", n.cfg.ID, "term", n.state.Term, "votes", n.votes)
}
