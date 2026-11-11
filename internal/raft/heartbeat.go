package raft

// Heartbeat: лидер периодически рассылает пустой AppendEntries,
// а follower по нему признаёт лидера и откладывает выборы.

import (
	"context"
	"errors"
	"sync"
)

// HandleHeartbeat принимает сообщение лидера. Устаревший срок отклоняется.
func (n *Node) HandleHeartbeat(req HeartbeatRequest) (HeartbeatResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failure != nil {
		return HeartbeatResponse{}, n.failure
	}
	if !n.member(req.LeaderID) {
		return HeartbeatResponse{}, errors.New("unknown leader")
	}
	if err := n.observeTermLocked(req.Term); err != nil {
		return HeartbeatResponse{}, err
	}
	res := HeartbeatResponse{Term: n.state.Term}
	if req.Term < n.state.Term {
		return res, nil
	}
	// Признаём лидера и откладываем выборы. Голос того же срока не очищаем.
	n.role, n.leaderID, n.votes = Follower, req.LeaderID, 0
	n.resetDeadlineLocked()
	res.Success = true
	return res, nil
}

// Рассылка heartbeat — отдельный HTTP-запрос каждому peer, включая недоступных.
// Ответы обрабатываются независимо; кворум heartbeat в этой версии не считается.
func (n *Node) broadcastHeartbeats(ctx context.Context, workers *sync.WaitGroup, term uint64) {
	for _, address := range n.cfg.Peers {
		workers.Add(1)
		go func(address string) {
			defer workers.Done()
			n.sendHeartbeat(ctx, address, term)
		}(address)
	}
}

// sendHeartbeat отправляет одно сообщение; больший срок в ответе заставит уступить.
func (n *Node) sendHeartbeat(ctx context.Context, address string, term uint64) {
	res, err := n.transport.Heartbeat(ctx, address, HeartbeatRequest{Term: term, LeaderID: n.cfg.ID})
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.failure == nil {
		_ = n.observeTermLocked(res.Term)
	}
}
