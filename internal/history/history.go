package history

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/GabboPenna/kx/internal/store"
)

type Entry struct {
	ID        string        `json:"id"`
	StartedAt time.Time     `json:"startedAt"`
	Selector  string        `json:"selector"`
	Command   []string      `json:"command"`
	Results   []Result      `json:"results"`
	Duration  time.Duration `json:"duration"`
}

type Result struct {
	Context  string        `json:"context"`
	ExitCode int           `json:"exitCode"`
	Stdout   string        `json:"stdout,omitempty"`
	Stderr   string        `json:"stderr,omitempty"`
	Duration time.Duration `json:"duration"`
}

func EntryFromResults(started time.Time, selector string, command []string, results []Result) Entry {
	return Entry{
		ID:        started.UTC().Format("20060102T150405.000000000Z"),
		StartedAt: started,
		Selector:  selector,
		Command:   append([]string(nil), command...),
		Results:   results,
		Duration:  time.Since(started),
	}
}

func Append(entry Entry) error {
	path, err := path()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func LoadRecent(limit int) ([]Entry, error) {
	path, err := path()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []Entry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err == nil {
			entries = append(entries, entry)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[len(entries)-limit:]
	}
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
	return entries, nil
}

func path() (string, error) {
	dir, err := store.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "history.jsonl"), nil
}
