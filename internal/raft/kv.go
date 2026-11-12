package raft

// Машина состояний: key-value хранилище в памяти.
// Каждый узел применяет к ней зафиксированные записи журнала строго по порядку.
// Одинаковый журнал и детерминированные команды дают одинаковое состояние на всех узлах.

import (
	"errors"
	"strings"
)

// Команды: "set <ключ> <значение>", "get <ключ>", "delete <ключ>".
// Значение может содержать пробелы, ключ — нет.
const (
	OpSet    = "set"
	OpGet    = "get"
	OpDelete = "delete"
)

// Command — разобранная команда машины состояний.
type Command struct {
	Op    string
	Key   string
	Value string
}

// ParseCommand разбирает строку команды. Вызывается перед добавлением в журнал,
// чтобы некорректная команда не попала в журнал всего кластера.
func ParseCommand(s string) (Command, error) {
	op, rest, _ := strings.Cut(s, " ")
	key, value, hasValue := strings.Cut(rest, " ")
	switch {
	case key == "":
		return Command{}, errors.New(`command must be "set <key> <value>", "get <key>" or "delete <key>"`)
	case op == OpSet && hasValue:
		return Command{Op: op, Key: key, Value: value}, nil
	case (op == OpGet || op == OpDelete) && !hasValue:
		return Command{Op: op, Key: key}, nil
	}
	return Command{}, errors.New(`command must be "set <key> <value>", "get <key>" or "delete <key>"`)
}

// Result — результат выполнения команды, его получает клиент.
// set: found=true и новое значение; get: значение и найден ли ключ;
// delete: был ли ключ до удаления.
type Result struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	Error string `json:"error,omitempty"`
}

// KV не защищён своим mutex: узел вызывает его методы под своим mutex.
type KV struct{ data map[string]string }

func NewKV() KV { return KV{data: make(map[string]string)} }

// Apply выполняет одну команду. Результат зависит только от команды и текущего
// состояния, поэтому на всех узлах получается одно и то же.
func (kv KV) Apply(s string) Result {
	cmd, err := ParseCommand(s)
	if err != nil {
		return Result{Error: err.Error()}
	}
	switch cmd.Op {
	case OpSet:
		kv.data[cmd.Key] = cmd.Value
		return Result{Value: cmd.Value, Found: true}
	case OpGet:
		value, ok := kv.data[cmd.Key]
		return Result{Value: value, Found: ok}
	default: // OpDelete
		_, ok := kv.data[cmd.Key]
		delete(kv.data, cmd.Key)
		return Result{Found: ok}
	}
}

// Snapshot возвращает копию данных для просмотра через GET /kv.
func (kv KV) Snapshot() map[string]string { return copyMap(kv.data) }
