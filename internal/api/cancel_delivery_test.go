package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"raincut/internal/incident"
	"raincut/internal/lifecycle"
)

// failingWriter is a ResponseWriter whose body write fails after the status
// is committed, the way a connection that disappears during the response
// behaves. Flush is accepted so writeJSON's flusher path is exercised too.
type failingWriter struct {
	header http.Header
	status int
}

func (f *failingWriter) Header() http.Header {
	if f.header == nil {
		f.header = make(http.Header)
	}
	return f.header
}

func (f *failingWriter) WriteHeader(status int) { f.status = status }

func (f *failingWriter) Write(p []byte) (int, error) {
	return 0, errConnectionBroken
}

func (f *failingWriter) Flush() {}

var errConnectionBroken = fmt.Errorf("write: connection reset by peer")

// validIncidentBody is a small chain event: release 0@0, intake 2, deadline
// 10. Arrivals at 5 are 0@0, 1@1, 2@2 (breached).
const validIncidentBody = `{"n":3,"pipes":[{"from":0,"to":1,"minutes":1},{"from":1,"to":2,"minutes":1}],"releases":[{"node":0,"at":0}],"intakes":[2],"deadline":10}`

// TestCreateCommittedOnlyAfterDelivery pins the delivery boundary of POST
// /incidents: a successful create leaves exactly one incident in the store,
// but a create whose response write fails leaves the prepared event out of
// the store — no orphan with an identifier the caller never received.
func TestCreateCommittedOnlyAfterDelivery(t *testing.T) {
	store := incident.NewStore()
	mux := newMux(lifecycle.NewGate(), store)

	// Successful delivery: exactly one stored event.
	req := httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(validIncidentBody))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", rec.Code, rec.Body.String())
	}
	if store.Len() != 1 {
		t.Fatalf("store len=%d after delivered create, want 1", store.Len())
	}

	// Response write fails after the status is committed: the prepared event
	// must never be committed.
	req = httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(validIncidentBody))
	fw := &failingWriter{}
	mux.ServeHTTP(fw, req)
	if fw.status != http.StatusOK {
		t.Fatalf("failed-write create status=%d, want the 200 attempted", fw.status)
	}
	if store.Len() != 1 {
		t.Fatalf("undelivered create changed the store: len=%d, want still 1 (no orphan)", store.Len())
	}
}

// TestCreateCancelledDuringBuild races a cancellation against the large
// propagation build. Every race must finish promptly, leave no incident
// behind and release its drain lease. An uncancelled create of the same
// payload still succeeds, so the ordinary path is unchanged.
func TestCreateCancelledDuringBuild(t *testing.T) {
	store := incident.NewStore()
	gate := lifecycle.NewGate()
	mux := newMux(gate, store)
	body := largeIncidentPayload(t)

	const races = 8
	observedCancel := false
	for i := 0; i < races; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(i)*100*time.Microsecond, cancel)
		req := httptest.NewRequest(http.MethodPost, "/incidents", bytes.NewReader(body)).WithContext(ctx)
		rec := newSafeRecorder()
		done := make(chan struct{})
		go func() {
			mux.ServeHTTP(rec, req)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled create handler never returned")
		}
		if rec.code() == 0 {
			observedCancel = true // no response written for a cancelled request
		} else if rec.code() != http.StatusOK {
			t.Fatalf("create race status=%d body=%s", rec.code(), rec.body())
		}
		cancel()
	}
	if !observedCancel {
		t.Fatal("no race observed cancellation during the build")
	}
	if n := store.Len(); n != 0 {
		t.Fatalf("cancelled creates left %d orphan events, want 0", n)
	}
	if n := gate.InFlight(); n != 0 {
		t.Fatalf("cancelled creates hold %d leases, want 0", n)
	}

	// The same payload without cancellation still creates normally.
	req := httptest.NewRequest(http.MethodPost, "/incidents", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ordinary large create status=%d body=%s", rec.Code, rec.Body.String())
	}
	if store.Len() != 1 {
		t.Fatalf("store len=%d after ordinary create, want 1", store.Len())
	}
}

// TestCreatePreCancelledNeverTouchesStore: a context already cancelled
// before the handler runs returns silently, writes nothing and leaves the
// store empty for both business routes.
func TestCreatePreCancelledNeverTouchesStore(t *testing.T) {
	store := incident.NewStore()
	mux := newMux(lifecycle.NewGate(), store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(validIncidentBody)).WithContext(ctx)
	rec := newSafeRecorder()
	mux.ServeHTTP(rec, req)
	if rec.code() != 0 {
		t.Fatalf("pre-cancelled create wrote status %d, want no response", rec.code())
	}
	if store.Len() != 0 {
		t.Fatalf("pre-cancelled create stored %d events, want 0", store.Len())
	}
}

// TestMinCutCancelledReleasesLease races cancellation against a large
// minimum-cut solve: the handler must return promptly (not run the graph to
// completion) with no lease left behind, while an uncancelled solve of the
// same graph still reports the exact answer.
func TestMinCutCancelledReleasesLease(t *testing.T) {
	gate := lifecycle.NewGate()
	mux := newMux(gate, incident.NewStore())
	body := largeMincutPayload(t)

	const races = 8
	observedCancel := false
	for i := 0; i < races; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Duration(i)*100*time.Microsecond, cancel)
		req := httptest.NewRequest(http.MethodPost, "/mincut", bytes.NewReader(body)).WithContext(ctx)
		rec := newSafeRecorder()
		done := make(chan struct{})
		go func() {
			mux.ServeHTTP(rec, req)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled mincut handler never returned")
		}
		switch rec.code() {
		case 0:
			observedCancel = true
		case http.StatusOK:
		default:
			t.Fatalf("mincut race status=%d body=%s", rec.code(), rec.body())
		}
		cancel()
	}
	if !observedCancel {
		t.Fatal("no race observed cancellation during the solve")
	}
	if n := gate.InFlight(); n != 0 {
		t.Fatalf("cancelled solves hold %d leases, want 0", n)
	}

	req := httptest.NewRequest(http.MethodPost, "/mincut", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ordinary large mincut status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp SolveResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("mincut response: %v", err)
	}
	if resp.MinimumShutdownCost <= 0 {
		t.Fatalf("mincut cost=%d, want positive", resp.MinimumShutdownCost)
	}
}

// TestAdvanceFailedDeliveryIsReplayed covers the domain boundary end to end
// through the HTTP handler: when the response of a committed advance cannot
// be delivered, a same-minute retry on the same incident replays the exact
// increment and snapshot; after a delivered retry the increment goes back to
// the ordinary empty idempotent response.
func TestAdvanceFailedDeliveryIsReplayed(t *testing.T) {
	store := incident.NewStore()
	mux := newMux(lifecycle.NewGate(), store)

	// Create the event through a delivered response.
	createReq := httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(validIncidentBody))
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)
	var created CreateIncidentResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("create response: %v", err)
	}
	id := created.ID

	advancePath := "/incidents/" + id + "/advance"
	body := strings.NewReader(`{"minute":5}`)

	// First commit: the connection dies during the response write.
	failed := httptest.NewRequest(http.MethodPost, advancePath, body)
	fw := &failingWriter{}
	mux.ServeHTTP(fw, failed)
	if fw.status != http.StatusOK {
		t.Fatalf("first advance status=%d, want 200 attempted", fw.status)
	}

	// Same-minute retry with a healthy writer must replay the increment.
	retry := httptest.NewRequest(http.MethodPost, advancePath, strings.NewReader(`{"minute":5}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, retry)
	if rec.Code != http.StatusOK {
		t.Fatalf("replay advance status=%d body=%s", rec.Code, rec.Body.String())
	}
	var first AdvanceResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatalf("replay response: %v", err)
	}
	want := []ArrivalDTO{{Node: 0, AtMinute: 0}, {Node: 1, AtMinute: 1}, {Node: 2, AtMinute: 2}}
	if len(first.NewArrivals) != 3 {
		t.Fatalf("replayed new_arrivals=%v, want the committed 3", first.NewArrivals)
	}
	for i := range want {
		if first.NewArrivals[i] != want[i] {
			t.Fatalf("replayed arrival %d=%+v, want %+v", i, first.NewArrivals[i], want[i])
		}
	}
	if first.Snapshot.CurrentMinute != 5 || first.Snapshot.Status != "breached" ||
		len(first.Snapshot.EarliestArrivals) != 3 {
		t.Fatalf("replayed snapshot=%+v, want minute 5 breached with 3 arrivals", first.Snapshot)
	}

	// Another same-minute retry now behaves normally: empty increment, same
	// snapshot.
	again := httptest.NewRequest(http.MethodPost, advancePath, strings.NewReader(`{"minute":5}`))
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, again)
	var second AdvanceResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &second); err != nil {
		t.Fatalf("post-replay retry: %v", err)
	}
	if len(second.NewArrivals) != 0 || second.Snapshot.CurrentMinute != 5 || second.Snapshot.Status != "breached" {
		t.Fatalf("post-replay retry=%+v, want empty arrivals at minute 5 breached", second)
	}

	// The error structure and conflict semantics are unchanged.
	back := httptest.NewRequest(http.MethodPost, advancePath, strings.NewReader(`{"minute":4}`))
	rec3 := httptest.NewRecorder()
	mux.ServeHTTP(rec3, back)
	if rec3.Code != http.StatusConflict {
		t.Fatalf("backwards status=%d, want 409 body=%s", rec3.Code, rec3.Body.String())
	}
	var er ErrorResponse
	if err := json.Unmarshal(rec3.Body.Bytes(), &er); err != nil || er.Error.Code != "clock_regression" {
		t.Fatalf("backwards error=%s, want clock_regression", rec3.Body.String())
	}
}

// TestUndeliveredIncrementDeliveredAtLeastOnce races one failing writer
// against many healthy same-minute clients. The delivery invariant is:
// whenever a committed non-empty increment fails its write, at least one
// later same-minute 200 response must still carry the arrivals — an empty
// retry delivered by another concurrent client must not erase the marker.
// A trailing request makes the invariant check exact; the old unconditional
// acknowledge (empty retry clears the marker) violates it.
func TestUndeliveredIncrementDeliveredAtLeastOnce(t *testing.T) {
	const trials = 40
	for trial := 0; trial < trials; trial++ {
		store := incident.NewStore()
		mux := newMux(lifecycle.NewGate(), store)

		createReq := httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(validIncidentBody))
		createRec := httptest.NewRecorder()
		mux.ServeHTTP(createRec, createReq)
		var created CreateIncidentResponse
		if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
			t.Fatalf("create response: %v", err)
		}
		path := "/incidents/" + created.ID + "/advance"

		const clients = 8
		var wg sync.WaitGroup
		var gotMu sync.Mutex
		nonEmptyDelivered := 0
		start := make(chan struct{})
		// Client 0's connection dies during the response write.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			mux.ServeHTTP(&failingWriter{},
				httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"minute":5}`)))
		}()
		for i := 1; i < clients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"minute":5}`)))
				if rec.Code == http.StatusOK {
					var resp AdvanceResponse
					if json.Unmarshal(rec.Body.Bytes(), &resp) == nil && len(resp.NewArrivals) > 0 {
						gotMu.Lock()
						nonEmptyDelivered++
						gotMu.Unlock()
					}
				}
			}()
		}
		close(start)
		wg.Wait()

		// Trailing same-minute request: if the failed commit's arrivals have
		// not been delivered to anyone yet, they must replay here.
		tail := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"minute":5}`))
		tailRec := httptest.NewRecorder()
		mux.ServeHTTP(tailRec, tail)
		var tailResp AdvanceResponse
		if err := json.Unmarshal(tailRec.Body.Bytes(), &tailResp); err != nil {
			t.Fatalf("trial %d: trailing retry: %v (%s)", trial, err, tailRec.Body.String())
		}
		if len(tailResp.NewArrivals) > 0 {
			nonEmptyDelivered++
		}
		if nonEmptyDelivered < 1 {
			t.Fatalf("trial %d: committed arrivals were delivered to nobody after a failed write", trial)
		}
	}
}

// TestCancelledAdvanceBeforeCommit: a request already cancelled before the
// domain call writes nothing and neither commits the clock nor holds a
// lease; the first delivered advance afterwards still carries the full
// increment.
func TestCancelledAdvanceBeforeCommit(t *testing.T) {
	store := incident.NewStore()
	gate := lifecycle.NewGate()
	mux := newMux(gate, store)

	createReq := httptest.NewRequest(http.MethodPost, "/incidents", strings.NewReader(validIncidentBody))
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)
	var created CreateIncidentResponse
	if err := json.Unmarshal(createRec.Body.Bytes(), &created); err != nil {
		t.Fatalf("create response: %v", err)
	}
	path := "/incidents/" + created.ID + "/advance"

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"minute":5}`)).WithContext(ctx)
	rec := newSafeRecorder()
	mux.ServeHTTP(rec, req)
	if rec.code() != 0 {
		t.Fatalf("pre-cancelled advance wrote status %d, want no response", rec.code())
	}
	if gate.InFlight() != 0 {
		t.Fatalf("pre-cancelled advance left %d leases", gate.InFlight())
	}

	// First real advance must still see the uncommitted full increment.
	good := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"minute":5}`))
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, good)
	var resp AdvanceResponse
	if err := json.Unmarshal(okRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("advance response: %v", err)
	}
	if len(resp.NewArrivals) != 3 || resp.Snapshot.CurrentMinute != 5 {
		t.Fatalf("advance after cancelled request=%+v, want 3 arrivals at 5", resp)
	}
}

// safeRecorder is a thread-safe httptest.ResponseRecorder for requests
// served from a goroutine whose context is cancelled: the server never
// writes concurrently, but this keeps the race detector quiet across the
// ServeHTTP goroutine and the test goroutine.
type safeRecorder struct {
	mu     sync.Mutex
	header http.Header
	status int
	buf    bytes.Buffer
}

func newSafeRecorder() *safeRecorder {
	return &safeRecorder{header: make(http.Header)}
}

func (s *safeRecorder) Header() http.Header { return s.header }

func (s *safeRecorder) WriteHeader(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == 0 {
		s.status = status
	}
}

func (s *safeRecorder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.buf.Write(p)
}

func (s *safeRecorder) code() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *safeRecorder) body() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// largeMincutPayload builds a 20000-node / 100000-edge /mincut body as
// accepted JSON. The solve runs long enough at these bounds for a raced
// cancellation to land while Dinic is working.
func largeMincutPayload(t *testing.T) []byte {
	t.Helper()
	const n = 20000
	const m = 100000
	state := uint64(20260916)
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state >> 11
	}
	edges := make([]EdgeInput, 0, m)
	for i := int64(2); i <= 5000; i++ {
		edges = append(edges, EdgeInput{From: 0, To: i, Cost: 1_000_000_000})
		edges = append(edges, EdgeInput{From: i, To: 1, Cost: int64(next()%1000) + 1})
	}
	for len(edges) < m {
		u := int64(2 + next()%uint64(n-2))
		v := int64(2 + next()%uint64(n-2))
		edges = append(edges, EdgeInput{From: u, To: v, Cost: 1_000_000_000})
	}
	raw, err := json.Marshal(SolveRequest{N: n, Edges: edges, Sources: []int64{0}, Sinks: []int64{1}})
	if err != nil {
		t.Fatalf("marshal large mincut payload: %v", err)
	}
	return raw
}

// largeIncidentPayload builds a 20000-node / 100000-pipe /incidents body.
func largeIncidentPayload(t *testing.T) []byte {
	t.Helper()
	const n = 20000
	const m = 100000
	state := uint64(20260917)
	next := func() uint64 {
		state = state*6364136223846793005 + 1442695040888963407
		return state >> 11
	}
	pipes := make([]PipeInput, 0, m)
	for len(pipes) < m {
		u := int64(next() % n)
		v := int64(next() % n)
		pipes = append(pipes, PipeInput{From: u, To: v, Minutes: int64(1 + next()%1000)})
	}
	req := CreateIncidentRequest{
		N: n, Pipes: pipes, Releases: []ReleaseInput{{Node: 0, At: 0}},
		Intakes: []int64{n - 1}, Deadline: 1_000_000_000,
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal large incident payload: %v", err)
	}
	return raw
}
