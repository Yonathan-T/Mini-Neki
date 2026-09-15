package config

import (
	"encoding/json"
	"os"
)

func Load(path string) (*Topology, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var topology Topology
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&topology); err != nil {
		return nil, err
	}

	return &topology, nil
}
