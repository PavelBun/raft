package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// PersistentState переживает перезапуск (Figure 2: currentTerm, votedFor, log[]).
// Забытый голос позволил бы проголосовать дважды в одном сроке, а забытый
// журнал — потерять запись, которую лидер уже посчитал зафиксированной.
type PersistentState struct {
	Term     uint64  `json:"term"`
	VotedFor string  `json:"voted_for"`
	Log      []Entry `json:"log"`
}

// Интерфейс позволяет использовать файл в приложении и память в тестах.
type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// У каждого узла свой файл state.json и свой каталог данных.
type FileStorage struct{ Path string }

// Load вызывается при создании Node. Отсутствие файла означает первый запуск.
// Повреждённый файл возвращает ошибку: молча забывать предыдущий голос нельзя.
func (s FileStorage) Load() (PersistentState, error) {
	var state PersistentState
	b, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(b, &state); err != nil {
		return state, fmt.Errorf("read state: %w", err)
	}
	return state, nil
}

// Save вызывается ядром до подтверждения голоса, до ответа success на
// AppendEntries и до рассылки новых записей лидером. Файл перезаписывается
// целиком: для учебного журнала этого достаточно.
func (s FileStorage) Save(state PersistentState) error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0755); err != nil {
		return err
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	// Сначала записываем временный файл, чтобы не перезаписывать целевой по частям.
	f, err := os.CreateTemp(filepath.Dir(s.Path), ".raft-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	// Просим ОС синхронизировать данные файла перед его закрытием и переименованием.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Гарантии при отключении питания зависят от ОС и файловой системы.
	return os.Rename(name, s.Path)
}
