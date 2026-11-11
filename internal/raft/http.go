package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// HTTPTransport передаёт сообщения Raft через HTTP/JSON; правила алгоритма в Node.
type HTTPTransport struct{ Client *http.Client }

// call кодирует запрос в JSON, отправляет POST и декодирует JSON-ответ.
// Контекст позволяет отменить ожидание, а таймаут настроен у Client в main.go.
func (t HTTPTransport) call(ctx context.Context, address, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close() // Освобождаем тело ответа и ресурсы соединения.
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("RPC %s: HTTP %d", path, res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 65536)).Decode(output)
}

func (t HTTPTransport) RequestVote(ctx context.Context, address string, req VoteRequest) (VoteResponse, error) {
	var res VoteResponse
	err := t.call(ctx, address, "/raft/request-vote", req, &res)
	return res, err
}

func (t HTTPTransport) Heartbeat(ctx context.Context, address string, req HeartbeatRequest) (HeartbeatResponse, error) {
	var res HeartbeatResponse
	err := t.call(ctx, address, "/raft/append-entries", req, &res)
	return res, err
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

// Принимаем один JSON-объект размером до 64 КиБ, без неизвестных полей.
func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected a single JSON object")
	}
	return nil
}

// Handler регистрирует ручки. net/http может обрабатывать запросы конкурентно,
// поэтому методы Node сами защищают общее состояние mutex-ом.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	// Пользовательский API: снимок локального состояния, а не опрос всего кластера.
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		status := n.Status()
		code := http.StatusOK
		if status.Error != "" {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, status)
	})
	// Входящий запрос голоса: JSON → проверка полей → ядро Raft → JSON-ответ.
	mux.HandleFunc("POST /raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		var req VoteRequest
		if err := decodeJSON(w, r, &req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if req.Term == 0 || !n.member(req.CandidateID) {
			writeJSON(w, 400, map[string]string{"error": "positive term and known candidate_id required"})
			return
		}
		res, err := n.HandleVote(req)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, res)
	})
	// Пока AppendEntries используется только как heartbeat, без репликации журнала.
	mux.HandleFunc("POST /raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var req HeartbeatRequest
		if err := decodeJSON(w, r, &req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if req.Term == 0 || !n.member(req.LeaderID) {
			writeJSON(w, 400, map[string]string{"error": "positive term and known leader_id required"})
			return
		}
		res, err := n.HandleHeartbeat(req)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, res)
	})
	return mux
}
