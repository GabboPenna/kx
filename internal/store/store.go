package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type Metadata struct {
	ContextTags map[string]map[string]string `json:"contextTags"`
}

func Load() (Metadata, error) {
	path, err := metadataPath()
	if err != nil {
		return Metadata{}, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Metadata{ContextTags: map[string]map[string]string{}}, nil
	}
	if err != nil {
		return Metadata{}, err
	}
	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return Metadata{}, err
	}
	if meta.ContextTags == nil {
		meta.ContextTags = map[string]map[string]string{}
	}
	return meta, nil
}

func Save(meta Metadata) error {
	path, err := metadataPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func Dir() (string, error) {
	if home := os.Getenv("KX_HOME"); home != "" {
		return home, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "kx"), nil
}

func metadataPath() (string, error) {
	dir, err := Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "contexts.json"), nil
}
