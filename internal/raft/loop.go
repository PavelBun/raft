package raft

// Цикл узла: тикер проверяет сроки, а решения о выборах и heartbeat
// принимаются в handleTick. Сами правила Raft — в election.go и heartbeat.go.

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Run вызывается один раз и управляет жизненным циклом: ждёт тик или отмену.
// Решения о выборах и heartbeat вынесены в handleTick и отдельные методы.
func (n *Node) Run(ctx context.Context) error {
	n.mu.Lock()
	if n.running {
		n.mu.Unlock()
		return errors.New("node already started")
	}
	n.running = true
	n.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	// При остановке отменяем запросы и ждём исходящие goroutine.
	// WaitGroup не используется для ожидания всех голосов в каждом раунде.
	defer func() { cancel(); workers.Wait() }()
	// Тикер периодически присылает время в канал ticker.C.
	// Каждые 10 мс мы лишь проверяем, истёк ли таймаут выборов или пора ли
	// отправлять heartbeat. Это НЕ выборы и НЕ рассылка каждые 10 мс:
	// их интервалы задаются отдельно в cfg (по умолчанию 600–1200 и 100 мс).
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if err := n.handleTick(ctx, now, &workers); err != nil {
				return err
			}
		}
	}
}

// handleTick проверяет сроки действий под mutex, затем запускает сеть без mutex.
// Вызывается только из Run: добавление задач в workers не пересекается с Wait.
func (n *Node) handleTick(ctx context.Context, now time.Time, workers *sync.WaitGroup) error {
	n.mu.Lock()
	if n.failure != nil {
		err := n.failure
		n.mu.Unlock()
		return err
	}
	if n.role != Leader && !now.Before(n.deadline) {
		if err := n.startElectionLocked(); err != nil {
			n.mu.Unlock()
			return err
		}
		term := n.state.Term
		n.mu.Unlock()
		n.broadcastVoteRequests(ctx, workers, term)
		return nil
	}
	if n.role == Leader && !now.Before(n.nextHeartbeat) {
		term := n.state.Term
		n.nextHeartbeat = now.Add(n.cfg.Heartbeat)
		n.mu.Unlock()
		n.broadcastHeartbeats(ctx, workers, term)
		return nil
	}
	n.mu.Unlock()
	return nil
}
