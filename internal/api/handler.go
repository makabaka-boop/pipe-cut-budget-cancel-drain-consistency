// Package api exposes the HTTP interface of the raincut service.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"raincut/internal/flow"
	"raincut/internal/incident"
	"raincut/internal/lifecycle"
)

const (
	maxNodes     = 20000
	maxEdges     = 100000
	maxCost      = int64(1_000_000_000)
	maxBodyBytes = 32 << 20
)

// EdgeInput is one directed pipe in the request payload.
type EdgeInput struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
	Cost int64 `json:"cost"`
}

// SolveRequest is the payload accepted by POST /mincut.
type SolveRequest struct {
	N       int64       `json:"n"`
	Edges   []EdgeInput `json:"edges"`
	Sources []int64     `json:"sources"`
	Sinks   []int64     `json:"sinks"`
}

// SolveResponse carries the minimum shutdown cost.
type SolveResponse struct {
	MinimumShutdownCost int64 `json:"minimum_shutdown_cost"`
}

// ErrorResponse is the stable error structure returned for any rejected
// request.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes why a request was rejected.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// server holds the shared, concurrency-safe dependencies of the routes.
type server struct {
	incidents *incident.Store
	gate      *lifecycle.Gate
}

// NewMux builds the HTTP routing table with its own lifecycle gate. Tests
// that never drive a drain use it directly; the real process shares one gate
// between the mux and the signal handler via NewMuxWithGate.
func NewMux() http.Handler {
	return NewMuxWithGate(lifecycle.NewGate())
}

// NewMuxWithGate builds the HTTP routing table sharing the given admission
// gate. Business routes (/mincut, /incidents, /incidents/{id}/advance) are
// wrapped by the barrier; the /healthz and /readyz probes bypass it.
func NewMuxWithGate(g *lifecycle.Gate) http.Handler {
	return newMux(g, incident.NewStore())
}

// newMux wires the routes of a server built around the shared gate and
// store. Tests supply their own store so they can count committed events
// across cancelled and failed requests.
func newMux(g *lifecycle.Gate, store *incident.Store) http.Handler {
	mux := http.NewServeMux()
	s := &server{incidents: store, gate: g}
	mux.HandleFunc("/mincut", s.admit(handleMinCut))
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/incidents", s.admit(s.handleCreateIncident))
	mux.HandleFunc("/incidents/{id}/advance", s.admit(s.handleAdvance))
	return mux
}

// admit enforces the drain barrier on a business route. While the gate is
// accepting the request takes a lease and runs to completion — even if its
// body is still arriving when the barrier falls. Once draining, the request
// never enters the handler, mutates no state, and gets a stable 503. The
// lease is released when the handler returns, which for a request whose
// caller went away happens as soon as the cancellation-aware handler
// unwinds, so an abandoned computation cannot hold the drain open.
func (s *server) admit(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		release, ok := s.gate.Acquire()
		if !ok {
			writeError(w, http.StatusServiceUnavailable, "draining",
				"service is draining and no longer accepts new requests")
			return
		}
		defer release()
		h(w, r)
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports readiness: 200 while the gate accepts requests, 503
// with status "draining" once the drain barrier has fallen.
func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.gate.State() == lifecycle.Accepting {
		writeJSON(w, http.StatusOK, map[string]string{"status": "accepting"})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
}

func handleMinCut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req SolveRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	edges, sources, sinks, msg := validate(&req)
	if msg != "" {
		writeError(w, http.StatusUnprocessableEntity, "invalid_input", msg)
		return
	}
	// The solver observes the request context: if the caller abandons the
	// page while a large cut is still running, the computation unwinds and
	// releases the drain lease immediately instead of running to completion.
	cost, err := flow.MinShutdownCostCtx(r.Context(), int(req.N), edges, sources, sinks)
	if err != nil {
		if clientGone(r, err) {
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "minimum cut failed")
		return
	}
	writeJSON(w, http.StatusOK, SolveResponse{MinimumShutdownCost: cost})
}

// validate checks every constraint; on failure it returns a human-readable
// message and the request never reaches the solver.
func validate(req *SolveRequest) ([]flow.Edge, []int, []int, string) {
	if req.N < 2 || req.N > maxNodes {
		return nil, nil, nil, fmt.Sprintf("n must satisfy 2 <= n <= %d, got %d", maxNodes, req.N)
	}
	n := int(req.N)
	if len(req.Edges) > maxEdges {
		return nil, nil, nil, fmt.Sprintf("at most %d edges allowed, got %d", maxEdges, len(req.Edges))
	}
	edges := make([]flow.Edge, 0, len(req.Edges))
	for i, e := range req.Edges {
		switch {
		case e.From < 0 || e.From >= req.N:
			return nil, nil, nil, fmt.Sprintf("edges[%d].from=%d is out of range [0,%d)", i, e.From, n)
		case e.To < 0 || e.To >= req.N:
			return nil, nil, nil, fmt.Sprintf("edges[%d].to=%d is out of range [0,%d)", i, e.To, n)
		case e.Cost < 1 || e.Cost > maxCost:
			return nil, nil, nil, fmt.Sprintf("edges[%d].cost=%d must satisfy 1 <= cost <= %d", i, e.Cost, maxCost)
		}
		edges = append(edges, flow.Edge{From: int(e.From), To: int(e.To), Cost: e.Cost})
	}
	if len(req.Sources) == 0 {
		return nil, nil, nil, "sources must be a non-empty array of node ids"
	}
	if len(req.Sinks) == 0 {
		return nil, nil, nil, "sinks must be a non-empty array of node ids"
	}
	isSource := make([]bool, n)
	sources := make([]int, 0, len(req.Sources))
	for i, id := range req.Sources {
		if id < 0 || id >= req.N {
			return nil, nil, nil, fmt.Sprintf("sources[%d]=%d is out of range [0,%d)", i, id, n)
		}
		isSource[int(id)] = true
		sources = append(sources, int(id))
	}
	sinks := make([]int, 0, len(req.Sinks))
	for i, id := range req.Sinks {
		if id < 0 || id >= req.N {
			return nil, nil, nil, fmt.Sprintf("sinks[%d]=%d is out of range [0,%d)", i, id, n)
		}
		if isSource[int(id)] {
			return nil, nil, nil, fmt.Sprintf("node %d cannot be both a source and a sink", id)
		}
		sinks = append(sinks, int(id))
	}
	return edges, sources, sinks, ""
}

// writeJSON serialises v and delivers status plus the complete body in one
// write. Encoding happens before any byte reaches the client, so an encoding
// failure can still produce a stable 500 instead of a truncated 200 response.
// Failures of the actual connection write are returned to the caller, which
// must treat the operation result as undelivered (the domain layer keeps
// committed advances recoverable; creates are only committed afterwards).
func writeJSON(w http.ResponseWriter, status int, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		log.Printf("api: encode response: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, werr := w.Write([]byte(`{"error":{"code":"internal_error","message":"could not encode response"}}`))
		return werr
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		return err
	}
	// Force the bytes onto the wire now. Without this, small bodies can sit
	// in the server's write buffer until the handler returns, which would
	// surface a broken connection only after the caller could act on the
	// failure.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

// writeError delivers the stable error structure and reports whether the
// error body actually reached the client. Callers that just wrote a response
// of their own need this signal to decide whether a committed result must be
// remembered as undelivered.
func writeError(w http.ResponseWriter, status int, code, message string) error {
	return writeJSON(w, status, ErrorResponse{Error: ErrorDetail{Code: code, Message: message}})
}

// clientGone reports whether err is the request giving up: its context was
// cancelled or hit its deadline (client closed the page / connection), or
// the body read/write itself failed because of that cancellation. Such a
// failure carries no deliverable response and must not change the stable
// error contract for any other failure.
func clientGone(r *http.Request, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if cerr := r.Context().Err(); cerr != nil {
		return true
	}
	return false
}
