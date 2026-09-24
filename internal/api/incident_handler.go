package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"raincut/internal/incident"
)

// PipeInput is one directed pipe with a traversal time for POST /incidents.
type PipeInput struct {
	From    int64 `json:"from"`
	To      int64 `json:"to"`
	Minutes int64 `json:"minutes"`
}

// ReleaseInput is one contamination release: node and release minute.
type ReleaseInput struct {
	Node int64 `json:"node"`
	At   int64 `json:"at"`
}

// CreateIncidentRequest is the payload accepted by POST /incidents.
type CreateIncidentRequest struct {
	N        int64          `json:"n"`
	Pipes    []PipeInput    `json:"pipes"`
	Releases []ReleaseInput `json:"releases"`
	Intakes  []int64        `json:"intakes"`
	Deadline int64          `json:"deadline"`
}

// AdvanceRequest is the payload accepted by POST /incidents/{id}/advance.
type AdvanceRequest struct {
	Minute int64 `json:"minute"`
}

// ArrivalDTO is a node reached for the first time at a given minute.
type ArrivalDTO struct {
	Node     int64 `json:"node"`
	AtMinute int64 `json:"at_minute"`
}

// SnapshotDTO is the committed clock state returned with every response.
type SnapshotDTO struct {
	CurrentMinute    int64            `json:"current_minute"`
	Status           string           `json:"status"`
	EarliestArrivals map[string]int64 `json:"earliest_arrivals"`
}

// CreateIncidentResponse is returned by POST /incidents: the identifier and
// the initial snapshot.
type CreateIncidentResponse struct {
	ID       string      `json:"id"`
	Snapshot SnapshotDTO `json:"snapshot"`
}

// AdvanceResponse is returned by POST /incidents/{id}/advance: the nodes
// reached for the first time since the previous committed minute and the new
// snapshot.
type AdvanceResponse struct {
	NewArrivals []ArrivalDTO `json:"new_arrivals"`
	Snapshot    SnapshotDTO  `json:"snapshot"`
}

// snapshotDTO converts a domain snapshot into the JSON shape, emitting
// earliest arrivals sorted by node so identical snapshots serialise
// identically (Go sorts map keys already, but the ordering is made explicit).
func snapshotDTO(s incident.Snapshot) SnapshotDTO {
	out := SnapshotDTO{
		CurrentMinute:    s.CurrentMinute,
		Status:           string(s.Status),
		EarliestArrivals: make(map[string]int64, len(s.EarliestArrivals)),
	}
	for node, minute := range s.EarliestArrivals {
		out.EarliestArrivals[fmt.Sprintf("%d", node)] = minute
	}
	return out
}

// decodeJSON reads exactly one JSON value into dst, rejecting unknown
// fields, trailing values and oversize bodies. It observes the request
// context: decoding a multi-megabyte topology can itself take longer than
// the client is willing to wait, so a request cancelled while the body is
// still being parsed aborts the decode rather than reading and parsing the
// whole body for nobody. On a non-cancellation failure it writes a stable
// 422 invalid_json. The boolean reports whether the caller may proceed.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	type decodeResult struct {
		err  error
		more bool
	}
	result := make(chan decodeResult, 1)
	go func() {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		err := dec.Decode(dst)
		// Owned solely by this goroutine until it reports: dec.More() must
		// not run on the caller's goroutine while the decoder state is still
		// hot, so the trailing-value check happens here.
		more := err == nil && dec.More()
		result <- decodeResult{err: err, more: more}
	}()
	select {
	case <-r.Context().Done():
		return false // client gone; the decoder finishes with a closed body and is discarded
	case res := <-result:
		if res.err != nil {
			writeError(w, http.StatusUnprocessableEntity, "invalid_json",
				"body must be a single JSON object with integer fields: "+res.err.Error())
			return false
		}
		if res.more {
			writeError(w, http.StatusUnprocessableEntity, "invalid_json",
				"body must contain exactly one JSON value")
			return false
		}
	}
	return true
}

// decodeStrict is the incident-route entry point kept for readability; it
// is exactly the cancellation-aware strict decode used by every JSON route.
func decodeStrict(w http.ResponseWriter, r *http.Request, dst any) bool {
	return decodeJSON(w, r, dst)
}

func (s *server) handleCreateIncident(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	var req CreateIncidentRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	spec, ok := buildSpec(w, &req)
	if !ok {
		return
	}
	// Validation and the shortest-path build run before the event exists;
	// both observe the request context, so a cancelled create aborts without
	// touching the store.
	in, err := s.incidents.Prepare(r.Context(), spec)
	if err != nil {
		if clientGone(r, err) {
			return
		}
		var ve *incident.ValidationError
		if errors.As(err, &ve) {
			writeError(w, http.StatusUnprocessableEntity, "invalid_input", ve.Msg)
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create incident")
		return
	}
	// Deliver the identifier first. Only once the response carrying the new
	// id has been written does the event enter the store: a create whose
	// response cannot be delivered leaves no orphan incident the caller could
	// never name or reach.
	resp := CreateIncidentResponse{ID: in.ID(), Snapshot: snapshotDTO(in.Snapshot())}
	if err := writeJSON(w, http.StatusOK, resp); err != nil {
		log.Printf("api: deliver create response for prepared incident %s: %v", in.ID(), err)
		return
	}
	s.incidents.Commit(in)
}

func (s *server) handleAdvance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "use POST")
		return
	}
	id := r.PathValue("id")
	in, exists := s.incidents.Get(id)
	if !exists {
		writeError(w, http.StatusNotFound, "not_found", fmt.Sprintf("incident %q does not exist", id))
		return
	}
	var req AdvanceRequest
	if !decodeStrict(w, r, &req) {
		return
	}
	if req.Minute < 0 {
		writeError(w, http.StatusUnprocessableEntity, "invalid_input",
			fmt.Sprintf("minute must be >= 0, got %d", req.Minute))
		return
	}
	// A request cancelled while queued behind another advance must not wait
	// for or observe a commit: it returns silently and its lease is released.
	if err := r.Context().Err(); err != nil {
		return
	}
	newArrivals, snap, err := in.Advance(req.Minute)
	if err != nil {
		if clientGone(r, err) {
			return
		}
		var ce *incident.ConflictError
		detail := err.Error()
		if errors.As(err, &ce) {
			detail = ce.Detail
		}
		switch {
		case errors.Is(err, incident.ErrClockRegression):
			writeError(w, http.StatusConflict, "clock_regression", detail)
		case errors.Is(err, incident.ErrPastDeadline):
			writeError(w, http.StatusConflict, "past_deadline", detail)
		case errors.Is(err, incident.ErrTerminal):
			writeError(w, http.StatusConflict, "incident_terminal", detail)
		default:
			writeError(w, http.StatusInternalServerError, "internal_error", "advance failed")
		}
		return
	}
	dto := make([]ArrivalDTO, 0, len(newArrivals))
	for _, a := range newArrivals {
		dto = append(dto, ArrivalDTO{Node: int64(a.Node), AtMinute: a.AtMinute})
	}
	// The domain commit is permanent, but the response may never reach the
	// caller. On a failed delivery of a non-empty increment remember the
	// exact committed outcome so the next same-minute retry replays it
	// instead of an empty list. A marker is only cleared once a response
	// carrying those arrivals is delivered: an ordinary empty same-minute
	// retry (possibly from a second concurrent client) must not erase a
	// marker left by someone else's failed write.
	writeErr := writeJSON(w, http.StatusOK, AdvanceResponse{NewArrivals: dto, Snapshot: snapshotDTO(snap)})
	if writeErr != nil {
		log.Printf("api: deliver advance response for incident %s minute %d: %v", id, req.Minute, writeErr)
		if len(newArrivals) > 0 {
			in.MarkUndelivered(req.Minute, newArrivals, snap)
		}
		return
	}
	if len(newArrivals) > 0 {
		in.AcknowledgeAdvance(req.Minute)
	}
}

// buildSpec converts the request into a domain spec; it reports a stable 422
// for any field outside the accepted integer domain.
func buildSpec(w http.ResponseWriter, req *CreateIncidentRequest) (incident.Spec, bool) {
	if req.N < 2 || req.N > incident.MaxNodes {
		return incident.Spec{}, invalidSpec(w, fmt.Sprintf("n must satisfy 2 <= n <= %d, got %d", incident.MaxNodes, req.N))
	}
	if req.Deadline < 0 || req.Deadline > incident.MaxMinutes {
		return incident.Spec{}, invalidSpec(w, fmt.Sprintf("deadline must satisfy 0 <= deadline <= %d, got %d", incident.MaxMinutes, req.Deadline))
	}
	n := int(req.N)
	if len(req.Pipes) > incident.MaxPipes {
		return incident.Spec{}, invalidSpec(w, fmt.Sprintf("at most %d pipes allowed, got %d", incident.MaxPipes, len(req.Pipes)))
	}
	spec := incident.Spec{N: n, Deadline: req.Deadline}
	spec.Pipes = make([]incident.Pipe, 0, len(req.Pipes))
	for i, p := range req.Pipes {
		if p.From < 0 || p.From >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("pipes[%d].from=%d is out of range [0,%d)", i, p.From, n))
		}
		if p.To < 0 || p.To >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("pipes[%d].to=%d is out of range [0,%d)", i, p.To, n))
		}
		if p.Minutes < 1 || p.Minutes > incident.MaxMinutes {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("pipes[%d].minutes=%d must satisfy 1 <= minutes <= %d", i, p.Minutes, incident.MaxMinutes))
		}
		spec.Pipes = append(spec.Pipes, incident.Pipe{From: int(p.From), To: int(p.To), Minutes: p.Minutes})
	}
	if len(req.Releases) == 0 {
		return incident.Spec{}, invalidSpec(w, "releases must be a non-empty array of {node, at} objects")
	}
	spec.Releases = make([]incident.Release, 0, len(req.Releases))
	for i, r := range req.Releases {
		if r.Node < 0 || r.Node >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("releases[%d].node=%d is out of range [0,%d)", i, r.Node, n))
		}
		if r.At < 0 || r.At > req.Deadline {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("releases[%d].at=%d must satisfy 0 <= at <= deadline %d", i, r.At, req.Deadline))
		}
		spec.Releases = append(spec.Releases, incident.Release{Node: int(r.Node), At: r.At})
	}
	if len(req.Intakes) == 0 {
		return incident.Spec{}, invalidSpec(w, "intakes must be a non-empty array of node ids")
	}
	spec.Intakes = make([]int, 0, len(req.Intakes))
	for i, id := range req.Intakes {
		if id < 0 || id >= req.N {
			return incident.Spec{}, invalidSpec(w, fmt.Sprintf("intakes[%d]=%d is out of range [0,%d)", i, id, n))
		}
		spec.Intakes = append(spec.Intakes, int(id))
	}
	return spec, true
}

func invalidSpec(w http.ResponseWriter, msg string) bool {
	writeError(w, http.StatusUnprocessableEntity, "invalid_input", msg)
	return false
}
