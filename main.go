package main

import (
	"MiniNeki/router"
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	shards := []router.Shard{
		{Name: "shard0", URL: "postgres://user:password@localhost:5433/shard0"},
		{Name: "shard1", URL: "postgres://user:password@localhost:5434/shard1"},
		{Name: "shard2", URL: "postgres://user:password@localhost:5435/shard2"},
	}
	for i := range shards {
		conn, err := pgx.Connect(context.Background(), shards[i].URL)
		if err != nil {
			log.Printf("failed to connect to %s: %v", shards[i].Name, err)
			continue
		}
		defer conn.Close(context.Background())
		shards[i].Conn = conn

		query := `CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			user_id INT NOT NULL UNIQUE,
			name VARCHAR(255) NOT NULL
		)`
		if _, err := conn.Exec(context.Background(), query); err != nil {
			log.Fatalf("failed to create table on %s: %v", shards[i].Name, err)
		}
		log.Printf("%s: table created", shards[i].Name)
	}

	sm := router.ShardMap{Shards: shards}
	if err := router.Insert(sm, 1, "Yonathan Taweke"); err != nil {
		log.Printf("failed to insert: %v", err)
	}
	if err := router.Query(sm, 1); err != nil {
		log.Printf("failed to query: %v", err)
	}
}
