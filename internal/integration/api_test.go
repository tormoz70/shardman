//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/api"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/resolve"
)

func newAPIServer(e *Env) *api.Server {
	return &api.Server{
		Store:      e.Store,
		ClusterKey: clusterKey,
		Resolver:   e.Resolver,
		RetSup:     e.Retention,
	}
}

func TestAPIBootstrapAnd409(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	srv := newAPIServer(e)
	body, _ := json.Marshal(map[string]any{
		"mode":            "hash",
		"bucket_axis":     "hash",
		"bucket_spec":     map[string]any{"bucket_count": 8, "hash_algo": "xxhash64"},
		"shard_max_bytes": 1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/bootstrap", bytes.NewReader(body))
	req.Header.Set("X-Cluster-Key", clusterKey)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("bootstrap status %d: %s", w.Code, w.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/v1/admin/bootstrap", bytes.NewReader(body))
	req2.Header.Set("X-Cluster-Key", clusterKey)
	w2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(w2, req2)
	if w2.Code != http.StatusConflict {
		t.Fatalf("second bootstrap status %d", w2.Code)
	}
}

func TestAPIUnauthorizedBootstrap(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	srv := newAPIServer(e)
	body, _ := json.Marshal(map[string]any{
		"mode":            "range",
		"bucket_axis":     "numeric",
		"bucket_spec":     map[string]int64{"width": 1000},
		"shard_max_bytes": 1024,
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/bootstrap", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", w.Code)
	}
}

func TestAPIResolveWrite503(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	srv := newAPIServer(e)

	body, _ := json.Marshal(map[string]int{"shard_key": 500})
	req := httptest.NewRequest(http.MethodPost, "/v1/resolve/write", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
}

func TestAPIResolveWriteAndSealRotate(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	maxBytes := int64(100)
	e.BootstrapNumeric(1000, maxBytes)
	e.RegisterStandbys(2)
	srv := newAPIServer(e)

	writeBody, _ := json.Marshal(map[string]int{"shard_key": 500})
	req := httptest.NewRequest(http.MethodPost, "/v1/resolve/write", bytes.NewReader(writeBody))
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("resolve %d: %s", w.Code, w.Body.String())
	}
	var wr resolve.WriteResult
	if err := json.Unmarshal(w.Body.Bytes(), &wr); err != nil {
		t.Fatal(err)
	}
	if wr.BucketID != "n0" || wr.Routing != bucket.RouteBucket {
		t.Fatalf("write %+v", wr)
	}

	if err := e.Store.UpdateStats(e.Ctx, e.ActiveUUID("n0"), maxBytes); err != nil {
		t.Fatal(err)
	}

	sealBody, _ := json.Marshal(map[string]string{"bucket_id": "n0"})
	sealReq := httptest.NewRequest(http.MethodPost, "/v1/admin/seal-rotate", bytes.NewReader(sealBody))
	sealReq.Header.Set("X-Cluster-Key", clusterKey)
	sealW := httptest.NewRecorder()
	srv.Router().ServeHTTP(sealW, sealReq)
	if sealW.Code != http.StatusOK {
		t.Fatalf("seal %d: %s", sealW.Code, sealW.Body.String())
	}

	readBody, _ := json.Marshal(map[string]int{"shard_key": 500})
	readReq := httptest.NewRequest(http.MethodPost, "/v1/resolve/read", bytes.NewReader(readBody))
	readW := httptest.NewRecorder()
	srv.Router().ServeHTTP(readW, readReq)
	if readW.Code != http.StatusOK {
		t.Fatalf("read %d", readW.Code)
	}
	var rr resolve.ReadResult
	if err := json.Unmarshal(readW.Body.Bytes(), &rr); err != nil {
		t.Fatal(err)
	}
	if len(rr.Shards) != 2 {
		t.Fatalf("read shards %+v", rr)
	}
}

func TestAPIRegisterShard(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	srv := newAPIServer(e)

	u := uuid.New()
	body, _ := json.Marshal(map[string]string{
		"shard_uuid":    u.String(),
		"dsn":           "postgres://api/db",
		"role":          "data",
		"startup_state": "standby",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/shards", bytes.NewReader(body))
	req.Header.Set("X-Cluster-Key", clusterKey)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("register %d: %s", w.Code, w.Body.String())
	}

	sh, err := e.Store.GetShardByUUID(e.Ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	if sh.State != fsm.StateStandby || sh.BucketID != nil {
		t.Fatalf("shard %+v", sh)
	}
}

func TestAPIBucketShardsList(t *testing.T) {
	e := OpenEnv(t)
	defer e.Close()

	e.BootstrapNumeric(1000, 1024)
	e.RegisterStandbys(1)
	_, err := e.Resolver.ResolveWrite(e.Ctx, 500)
	if err != nil {
		t.Fatal(err)
	}
	srv := newAPIServer(e)

	req := httptest.NewRequest(http.MethodGet, "/v1/buckets/n0/shards", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list %d", w.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["bucket_id"] != "n0" {
		t.Fatalf("resp %+v", resp)
	}
}
