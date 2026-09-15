package config

import (
	"testing"
)

func TestLoad(t *testing.T) {
	path := "../datatopology.json"
	topology, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if topology == nil {
		t.Fatal("topology is nil")
	}
	if topology.AuthoritativeShardGroup == "" {
		t.Fatal("authoritative shard group should not be empty")
	}

	if topology.Databases == nil {
		t.Fatal("databases map should not be nil")
	}
}
