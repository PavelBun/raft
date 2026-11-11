package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// Предел тела запроса и ответа. Должен вмещать maxEntriesPerRPC самых длинных команд.
	maxBodyBytes          = 1 << 20
	maxCommandBytes       = 4096
	maxCommandsPerRequest = 100
	// Сколько POST /log ждёт фиксации. Меньше WriteTimeout сервера в main.go.
	commitWaitTimeout = 2 * time.Second
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
	return json.NewDecoder(io.LimitReader(res.Body, maxBodyBytes)).Decode(output)
}

func (t HTTPTransport) RequestVote(ctx context.Context, address string, req VoteRequest) (VoteResponse, error) {
	var res VoteResponse
	err := t.call(ctx, address, "/raft/request-vote", req, &res)
	return res, err
}

func (t HTTPTransport) AppendEntries(ctx context.Context, address string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var res AppendEntriesResponse
	err := t.call(ctx, address, "/raft/append-entries", req, &res)
	return res, err
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

// Принимаем один JSON-объект размером до 1 МиБ, без неизвестных полей.
func decodeJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
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
	// Входящий AppendEntries: репликация записей или heartbeat с пустым entries.
	mux.HandleFunc("POST /raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesRequest
		if err := decodeJSON(w, r, &req); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if err := req.validate(); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		if !n.member(req.LeaderID) {
			writeJSON(w, 400, map[string]string{"error": "known leader_id required"})
			return
		}
		res, err := n.HandleAppendEntries(req)
		if err != nil {
			writeJSON(w, 503, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, res)
	})
	// Пользовательский API журнала: добавить записи и показать журнал этого узла.
	mux.HandleFunc("POST /log", n.handleAddEntries)
	mux.HandleFunc("GET /log", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, n.Log())
	})
	return mux
}

// Тело POST /log: {"command":"x"} или {"commands":["x","y"]}.
type addEntriesRequest struct {
	Command  string   `json:"command"`
	Commands []string `json:"commands"`
}

// Команды одного запроса или одна команда. Пустые и слишком длинные не принимаем:
// слишком большая запись не поместилась бы в AppendEntries, и репликация застряла бы.
func (r addEntriesRequest) commands() ([]string, error) {
	commands := r.Commands
	if r.Command != "" {
		if len(commands) > 0 {
			return nil, errors.New("use either command or commands")
		}
		commands = []string{r.Command}
	}
	if len(commands) == 0 || len(commands) > maxCommandsPerRequest {
		return nil, fmt.Errorf("from 1 to %d commands required", maxCommandsPerRequest)
	}
	for _, command := range commands {
		if command == "" || len(command) > maxCommandBytes {
			return nil, fmt.Errorf("each command must be from 1 to %d bytes", maxCommandBytes)
		}
	}
	return commands, nil
}

// handleAddEntries отвечает клиенту только после фиксации записей: как в Raft,
// успешный ответ означает, что записи уже есть у большинства и не потеряются.
func (n *Node) handleAddEntries(w http.ResponseWriter, r *http.Request) {
	var req addEntriesRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	commands, err := req.commands()
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	index, term, err := n.Propose(commands)
	var notLeader *NotLeaderError
	if errors.As(err, &notLeader) {
		// Не лидер: подсказываем клиенту, куда отправить запрос. Лидер может быть неизвестен.
		writeJSON(w, 409, map[string]string{"error": err.Error(), "leader_id": notLeader.LeaderID, "leader_address": notLeader.LeaderAddress})
		return
	}
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": err.Error()})
		return
	}
	first := index - uint64(len(commands)) + 1
	ctx, cancel := context.WithTimeout(r.Context(), commitWaitTimeout)
	defer cancel()
	err = n.WaitCommitted(ctx, index, term)
	switch {
	case err == nil:
		writeJSON(w, 200, map[string]any{"first_index": first, "last_index": index, "term": term, "committed": true})
	case errors.Is(err, ErrEntryLost):
		writeJSON(w, 503, map[string]any{"error": err.Error(), "first_index": first, "last_index": index, "term": term, "committed": false})
	default:
		// Например, нет большинства. Запись осталась в журнале и ещё может быть зафиксирована.
		writeJSON(w, 504, map[string]any{"error": "not committed within timeout, may still be committed", "first_index": first, "last_index": index, "term": term, "committed": false})
	}
}
