// Package router provides PostgreSQL shard routing. also idk i have to write comments for it to stop annoying me
package router

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

type Shard struct {
	Name string
	URL  string
	Conn *pgx.Conn
}

type ShardMap struct {
	Shards []Shard
}

func (m ShardMap) Select(key int) Shard {
	return m.Shards[key%len(m.Shards)]
}

func Insert(sm ShardMap, userID int, name string) error {
	shard := sm.Select(userID)
	_, err := shard.Conn.Exec(context.Background(), `INSERT INTO users (user_id, name) VALUES ($1, $2)`, userID, name)
	return err
}

func Query(sm ShardMap, userID int) error {
	shard := sm.Select(userID)
	rows, err := shard.Conn.Query(context.Background(), `SELECT id, user_id, name FROM users WHERE user_id = $1`, userID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, userID int
		var name string
		rows.Scan(&id, &userID, &name)
		log.Printf("user: %d, user_id: %d, name: %s, from: %s", id, userID, name, shard.Name)
	}
	return nil
}
