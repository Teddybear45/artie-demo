// Package api exposes the queue over HTTP with a four-endpoint JSON surface.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/teddybear45/artie-demo/internal/queue"
)

func New(reg *queue.Registry) http.Handler {
	h := handlers{reg: reg}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues", h.create)
	mux.HandleFunc("GET /queues/{name}", h.stats)
	mux.HandleFunc("POST /queues/{name}/messages", h.enqueue)
	mux.HandleFunc("POST /queues/{name}/dequeue", h.dequeue)
	return mux
}

type handlers struct {
	reg *queue.Registry
}

func (h handlers) create(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name  string      `json:"name"`
		Order queue.Order `json:"order"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	q, err := h.reg.Create(req.Name, req.Order)
	if errors.Is(err, queue.ErrExists) {
		fail(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	reply(w, http.StatusCreated, q.Stats())
}

func (h handlers) enqueue(w http.ResponseWriter, r *http.Request) {
	q, ok := h.reg.Get(r.PathValue("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	var req struct {
		Body         string `json:"body"`
		Priority     int    `json:"priority"`
		DelaySeconds int    `json:"delay_seconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}
	msg, err := q.Enqueue(req.Body, req.Priority, time.Duration(req.DelaySeconds)*time.Second)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	reply(w, http.StatusCreated, msg)
}

func (h handlers) dequeue(w http.ResponseWriter, r *http.Request) {
	q, ok := h.reg.Get(r.PathValue("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	msg, err := q.Dequeue()
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	if msg == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	reply(w, http.StatusOK, msg)
}

func (h handlers) stats(w http.ResponseWriter, r *http.Request) {
	q, ok := h.reg.Get(r.PathValue("name"))
	if !ok {
		http.NotFound(w, r)
		return
	}
	reply(w, http.StatusOK, q.Stats())
}

func reply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, err error) {
	reply(w, code, map[string]string{"error": err.Error()})
}
