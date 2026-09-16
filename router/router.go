package router

import (
	"MiniNeki/config"

	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Router struct {
	mu       sync.RWMutex
	Topology *config.Topology
	Shards   map[string]Shard
}

type Shard struct {
	Name string
	URL  string
	Pool *pgxpool.Pool
}

func (r *Router) selectShard(keyRanges []config.KeyRange, keyBytes []byte) Shard {
	h := xxhash.Sum64(keyBytes)
	hexVal := fmt.Sprintf("%02x", byte(h>>56))

	for _, kr := range keyRanges {
		start := kr.Start
		if start == "" {
			start = "00"
		}
		end := kr.End
		if end == "" {
			end = "ff"
		}
		if hexVal >= start && (hexVal < end || kr.End == "") {
			return r.Shards[kr.ShardUID]
		}
	}

	return r.Shards[keyRanges[0].ShardUID]
}
func (r *Router) Route(tblName string, key any) (Shard, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tbl, ok := r.Topology.Databases["postgres"].Schemas["public"].Tables[tblName]
	if !ok {
		return Shard{}, fmt.Errorf("table %s not found in topology", tblName)
	}

	var KeyBytes []byte
	switch v := key.(type) {
	case int:
		KeyBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(KeyBytes, uint64(v))
	case int64:
		KeyBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(KeyBytes, uint64(v))
	case float64:
		KeyBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(KeyBytes, uint64(v))
	case string:
		KeyBytes = []byte(v)
	case []byte:
		KeyBytes = v
	case nil:
		KeyBytes = nil
	default:
		return Shard{}, fmt.Errorf("unsupported key type: %T", key)
	}
	shardGroupName := tbl.ShardGroup
	for _, sg := range r.Topology.ShardGroups {
		if sg.UID == shardGroupName {
			if len(sg.KeyRanges) == 1 && sg.KeyRanges[0].End == "" {
				return r.Shards[sg.KeyRanges[0].ShardUID], nil
			}
			return r.selectShard(sg.KeyRanges, KeyBytes), nil
		}
	}
	return Shard{}, fmt.Errorf("shard group %s not found in topology", shardGroupName)
}

func (r *Router) Watcher(filepath string) {
	var lastModified time.Time
	if info, err := os.Stat(filepath); err == nil {
		lastModified = info.ModTime()
	}

	for {
		time.Sleep(1 * time.Second)
		info, err := os.Stat(filepath)
		if err != nil {
			continue
		}
		if info.ModTime().After(lastModified) {
			lastModified = info.ModTime()
			newTopology, err := config.Load(filepath)
			if err != nil {
				log.Printf("[Watcher] failed to reload topology: %v [...KEEPING CURRENT]", err)
				continue
			}
			r.mu.Lock()
			r.Topology = newTopology
			r.mu.Unlock()
			log.Printf("[Watcher] topology reloaded successfully")
		}
	}
}
