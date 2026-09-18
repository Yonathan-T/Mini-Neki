package main

import (
	"MiniNeki/config"
	"MiniNeki/router"
	"MiniNeki/server"

	"context"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	schemaSQL, err := os.ReadFile("schema.sql")
	if err != nil {
		log.Fatalf("failed to read schema.sql: %v", err)
	}

	urls := map[string]string{
		"shard1": "postgres://user:password@localhost:5433/shard1",
		"shard2": "postgres://user:password@localhost:5434/shard2",
		"shard3": "postgres://user:password@localhost:5435/shard3",
		"shard4": "postgres://user:password@localhost:5436/shard4",
	}
	shardsMap := make(map[string]router.Shard)
	for name, url := range urls {
		pool, err := pgxpool.New(context.Background(), url)
		if err != nil {
			log.Printf("failed to connect to %s: %v", name, err)
			continue
		}
		defer pool.Close()

		if _, err := pool.Exec(context.Background(), string(schemaSQL)); err != nil {
			log.Fatalf("failed to create table on %s: %v", name, err)
		}
		shardsMap[name] = router.Shard{Name: name, URL: url, Pool: pool}
		log.Printf("%s: Ready", name)
	}

	topo, err := config.Load("datatopology.json")
	if err != nil {
		log.Fatalf("failed to load topology: %v", err)
	}

	r := &router.Router{
		Topology: topo,
		Shards:   shardsMap,
	}
	go r.Watcher("./datatopology.json")

	s := server.New(r)
	http.HandleFunc("/execute", s.HandleExecute)

	log.Println("server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
