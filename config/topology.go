package config

type Topology struct {
	AuthoritativeShardGroup string                `json:"authoritative_shard_group"`
	ShardIndexes            map[string]ShardIndex `json:"shard_indexes"`
	ShardGroups             []ShardGroup          `json:"shard_groups"`
	Databases               map[string]Database   `json:"databases"`
}

type ShardIndex struct {
	Type    string   `json:"type"`
	Columns []string `json:"columns"`
}

type ShardGroup struct {
	UID               string     `json:"uid"`
	DefaultShardIndex string     `json:"default_shard_index,omitempty"`
	KeyRanges         []KeyRange `json:"key_ranges"`
}

type KeyRange struct {
	ShardUID string `json:"shard_uid"`
	Start    string `json:"start,omitempty"`
	End      string `json:"end,omitempty"`
}

type Database struct {
	Schemas map[string]Schema `json:"schemas"`
}

type Schema struct {
	Tables map[string]Table `json:"tables"`
}

type Table struct {
	ShardGroup string `json:"shard_group"`
}
