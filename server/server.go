// Package server provides the HTTP API boundary for MiniNeki.
package server

import (
	"MiniNeki/router"
	"encoding/json"
	"net/http"
)

type Server struct {
	router *router.Router
}

func New(r *router.Router) *Server {
	return &Server{router: r}
}

type ExecuteRequest struct {
	Table string        `json:"table"`
	Key   int           `json:"key"`
	Query string        `json:"query"`
	Args  []interface{} `json:"args"`
}

func (s *Server) HandleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	shard, err := s.router.Route(req.Table, req.Key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tag, err := shard.Conn.Exec(r.Context(), req.Query, req.Args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "Executed",
		"shard":  shard.Name,
		"rows":   tag.RowsAffected(),
	})
}
