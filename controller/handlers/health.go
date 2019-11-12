package handler

import (
	"net/http"

	"github.com/gorilla/mux"
)

// HealthHandler handles health check requests
type HealthHandler struct{}

// NewHealthHandler returns a new health handler
func NewHealthHandler() HealthHandler {
	return HealthHandler{}
}

// Register adds paths to router
func (h HealthHandler) Register(router *mux.Router) {
	router.HandleFunc("/health", h.Health)
}

// Health serves health requests
func (h HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
