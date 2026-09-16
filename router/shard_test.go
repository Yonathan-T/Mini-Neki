package router

import (
	"MiniNeki/config"
	"testing"
)

func TestSelect(t *testing.T) {
	topo, err := config.Load("../datatopology.json")
	if err != nil {
		t.Fatal(err)
	}

	shards := map[string]Shard{
		"shard1": {Name: "shard1"},
		"shard2": {Name: "shard2"},
		"shard3": {Name: "shard3"},
		"shard4": {Name: "shard4"},
	}
	r := &Router{
		Topology: topo,
		Shards:   shards,
	}

	cases := []struct {
		key      int
		expected string
	}{
		{0, "shard1"},
		{1, "shard3"},
		{2, "shard4"},
		{3, "shard1"},
		{32, "shard2"},
		{100, "shard2"},
	}
	for _, c := range cases {
		got, err := r.Route("customers", c.key)
		if err != nil {
			t.Errorf("key %d: expected %s, got error: %v", c.key, c.expected, err)
		}
		if got.Name != c.expected {
			t.Errorf("key %d: expected %s, got %s", c.key, c.expected, got.Name)
		}
	}
	custShard, _ := r.Route("customers", 1)
	orderShard, _ := r.Route("orders", 1)
	countryShard, _ := r.Route("countries", nil)

	if err != nil {
		t.Errorf("expected no error for global table, got %v", err)
	}
	if countryShard.Name != "shard1" {
		t.Errorf("expected no error for global table, got %v", err)
	}
	if custShard.Name != orderShard.Name {
		t.Errorf("Colocation fails! Customer table went to %s, Order went to %s", custShard.Name, orderShard.Name)
	}
	_, err = r.Route("nonExistentTableJustLikeMyWorkLifeBalance", 1)
	if err == nil {
		t.Errorf("expected error for non-existent table, got nil")
	}
}
