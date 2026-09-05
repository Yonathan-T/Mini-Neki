package router

import "testing"

func TestSelect(t *testing.T) {
	shards := []Shard{
		{Name: "shard0"},
		{Name: "shard1"},
		{Name: "shard2"},
	}
	sm := ShardMap{Shards: shards}

	cases := []struct {
		key      int
		expected string
	}{
		{0, "shard0"},
		{1, "shard1"},
		{2, "shard2"},
		{3, "shard0"},
		{100, "shard1"},
	}

	for _, c := range cases {
		got := sm.Select(c.key)
		if got.Name != c.expected {
			t.Errorf("key %d: expected %s, got %s", c.key, c.expected, got.Name)
		}
	}
}
