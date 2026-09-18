package router

import (
	"MiniNeki/config"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/jackc/pgx/v5"
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

type QueryResult struct {
	Status        string           `json:"status"`
	Mode          string           `json:"mode,omitempty"`
	Shard         string           `json:"shard,omitempty"`
	ShardsQueried []string         `json:"shards_queried,omitempty"`
	Count         int              `json:"count,omitempty"`
	Rows          int64            `json:"rows,omitempty"`
	Data          []map[string]any `json:"data,omitempty"`
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

func (r *Router) RouteAll(tblName string) ([]Shard, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tbl, ok := r.Topology.Databases["postgres"].Schemas["public"].Tables[tblName]
	if !ok {
		return nil, fmt.Errorf("table %s not found in topology", tblName)
	}

	shardGroupName := tbl.ShardGroup
	for _, sg := range r.Topology.ShardGroups {
		if sg.UID == shardGroupName {
			seen := make(map[string]bool)
			var shards []Shard
			for _, kr := range sg.KeyRanges {
				if !seen[kr.ShardUID] {
					seen[kr.ShardUID] = true
					if s, ok := r.Shards[kr.ShardUID]; ok {
						shards = append(shards, s)
					}
				}
			}
			if len(shards) == 0 {
				return nil, fmt.Errorf("no shards found for group %s", shardGroupName)
			}
			return shards, nil
		}
	}
	return nil, fmt.Errorf("shard group %s not found", shardGroupName)
}

func (r *Router) Execute(ctx context.Context, table string, key any, query string, args []any) (QueryResult, error) {
	var targetShards []Shard
	if key != nil {
		shard, err := r.Route(table, key)
		if err != nil {
			return QueryResult{}, err
		}
		targetShards = []Shard{shard}
	} else {
		shards, err := r.RouteAll(table)
		if err != nil {
			return QueryResult{}, err
		}
		targetShards = shards
	}

	trimmedQuery := strings.ToUpper(strings.TrimSpace(query))
	isSelect := strings.HasPrefix(trimmedQuery, "SELECT") || strings.HasPrefix(trimmedQuery, "WITH")

	// for a single shard execution
	if len(targetShards) == 1 {
		shard := targetShards[0]
		if isSelect {
			rows, err := shard.Pool.Query(ctx, query, args...)
			if err != nil {
				return QueryResult{}, err
			}
			defer rows.Close()

			data, err := pgx.CollectRows(rows, pgx.RowToMap)
			if err != nil {
				return QueryResult{}, err
			}
			if data == nil {
				data = []map[string]any{}
			}
			return QueryResult{
				Status: "OK",
				Shard:  shard.Name,
				Count:  len(data),
				Data:   data,
			}, nil
		}

		tag, err := shard.Pool.Exec(ctx, query, args...)
		if err != nil {
			return QueryResult{}, err
		}
		return QueryResult{
			Status: "Executed",
			Shard:  shard.Name,
			Rows:   tag.RowsAffected(),
		}, nil
	}

	// scatter-gather
	if isSelect {
		var wg sync.WaitGroup
		var mu sync.Mutex
		var allData []map[string]any
		var shardsQueried []string
		var firstErr error

		for _, sh := range targetShards {
			wg.Add(1)
			go func(shard Shard) {
				defer wg.Done()
				rows, err := shard.Pool.Query(ctx, query, args...)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				defer rows.Close()

				data, err := pgx.CollectRows(rows, pgx.RowToMap)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}

				mu.Lock()
				allData = append(allData, data...)
				shardsQueried = append(shardsQueried, shard.Name)
				mu.Unlock()
			}(sh)
		}

		wg.Wait()
		if firstErr != nil {
			return QueryResult{}, firstErr
		}
		if allData == nil {
			allData = []map[string]any{}
		}
		allData = sortResults(allData, query)
		return QueryResult{
			Status:        "OK",
			Mode:          "scatter-gather",
			ShardsQueried: shardsQueried,
			Count:         len(allData),
			Data:          allData,
		}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var totalRows int64
	var shardsQueried []string
	var firstErr error

	for _, sh := range targetShards {
		wg.Add(1)
		go func(shard Shard) {
			defer wg.Done()
			tag, err := shard.Pool.Exec(ctx, query, args...)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			totalRows += tag.RowsAffected()
			shardsQueried = append(shardsQueried, shard.Name)
			mu.Unlock()
		}(sh)
	}

	wg.Wait()
	if firstErr != nil {
		return QueryResult{}, firstErr
	}

	return QueryResult{
		Status:        "Executed",
		Mode:          "scatter-gather",
		ShardsQueried: shardsQueried,
		Rows:          totalRows,
	}, nil
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

func sortResults(data []map[string]any, query string) []map[string]any {
	upper := strings.ToUpper(query)

	idx := strings.Index(upper, "ORDER BY")
	if idx != -1 && len(data) > 1 {
		parts := strings.Fields(query[idx+len("ORDER BY"):])
		if len(parts) > 0 {
			col := strings.Trim(parts[0], ";,")
			isDesc := len(parts) > 1 && strings.ToUpper(strings.Trim(parts[1], ";,")) == "DESC"

			sort.SliceStable(data, func(i, j int) bool {
				a, b := data[i][col], data[j][col]
				switch va := a.(type) {
				case int64:
					if vb, ok := b.(int64); ok {
						if isDesc {
							return va > vb
						}
						return va < vb
					}
				case int:
					if vb, ok := b.(int); ok {
						if isDesc {
							return va > vb
						}
						return va < vb
					}
				case string:
					if vb, ok := b.(string); ok {
						if isDesc {
							return va > vb
						}
						return va < vb
					}
				case float64:
					if vb, ok := b.(float64); ok {
						if isDesc {
							return va > vb
						}
						return va < vb
					}
				}
				return false
			})
		}
	}

	limitIdx := strings.Index(upper, "LIMIT")
	if limitIdx != -1 {
		limitParts := strings.Fields(query[limitIdx+len("LIMIT"):])
		if len(limitParts) > 0 {
			if n, err := strconv.Atoi(strings.Trim(limitParts[0], ";,")); err == nil && n < len(data) {
				data = data[:n]
			}
		}
	}

	return data
}
