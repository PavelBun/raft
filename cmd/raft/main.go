package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"raft-study/internal/raft"
)

// Превращаем -peers "n2=http://...,n3=http://..." в таблицу ID → адрес.
// Самого себя, дубликаты и неправильные адреса не допускаем.
func parsePeers(input, self string) (map[string]string, error) {
	peers := make(map[string]string)
	if strings.TrimSpace(input) == "" {
		return peers, nil
	}
	addresses := make(map[string]bool)
	for _, entry := range strings.Split(input, ",") {
		id, address, ok := strings.Cut(strings.TrimSpace(entry), "=")
		id, address = strings.TrimSpace(id), strings.TrimRight(strings.TrimSpace(address), "/")
		u, err := url.Parse(address)
		if !ok || id == "" || id == self || err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return nil, fmt.Errorf("invalid peer %q; expected other-id=http://host:port", entry)
		}
		if _, exists := peers[id]; exists || addresses[address] {
			return nil, fmt.Errorf("duplicate peer %q", entry)
		}
		peers[id], addresses[address] = address, true
	}
	return peers, nil
}

// run собирает зависимости и управляет запуском/остановкой одного узла.
func run() error {
	id := flag.String("id", "", "Unique node ID, e.g. n1")
	listen := flag.String("listen", "127.0.0.1:8001", "HTTP listen address")
	peerList := flag.String("peers", "", "Other nodes: n2=http://127.0.0.1:8002,n3=http://127.0.0.1:8003")
	electionMin := flag.Duration("election-min", 600*time.Millisecond, "Minimum election timeout")
	electionMax := flag.Duration("election-max", 1200*time.Millisecond, "Maximum election timeout")
	heartbeat := flag.Duration("heartbeat", 100*time.Millisecond, "Heartbeat interval")
	rpcTimeout := flag.Duration("rpc-timeout", 250*time.Millisecond, "HTTP RPC timeout")
	flag.Parse()
	if *id == "" || *rpcTimeout <= 0 {
		return errors.New("-id and positive -rpc-timeout are required")
	}
	peers, err := parsePeers(*peerList, *id)
	if err != nil {
		return err
	}
	// Клиент отправляет исходящие RPC; сервер ниже принимает входящие запросы.
	client := &http.Client{Timeout: *rpcTimeout}
	defer client.CloseIdleConnections()
	// Одному Node передаём его конфигурацию и HTTP-транспорт. Всё состояние — в памяти.
	node, err := raft.New(raft.Config{ID: *id, Peers: peers, ElectionMin: *electionMin, ElectionMax: *electionMax, Heartbeat: *heartbeat},
		raft.HTTPTransport{Client: client}, slog.Default())
	if err != nil {
		return err
	}
	// Открываем порт до выборов: если порт занят, узел не начнёт участие в протоколе.
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	// Ctrl+C отменяет контекст, чтобы остановить цикл и исходящие запросы.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{Handler: node.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second}
	// Две основные задачи используют один Node. Каждая сообщит о завершении в канал.
	// Буфер на два результата позволяет им завершиться независимо друг от друга.
	results := make(chan error, 2)
	go func() { results <- server.Serve(listener) }()
	go func() { results <- node.Run(ctx) }()
	slog.Info("node started", "id", *id, "listen", *listen, "cluster_size", len(peers)+1)
	var result error
	completed := 0
	select {
	case <-ctx.Done():
	case result = <-results:
		completed++
	}
	// При завершении любой части останавливаем вторую и ждём обе.
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
	}
	for completed < 2 {
		err := <-results
		completed++
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	if errors.Is(result, http.ErrServerClosed) {
		return nil
	}
	return result
}

func main() {
	if err := run(); err != nil {
		slog.Error("node stopped", "error", err)
		os.Exit(1)
	}
}
