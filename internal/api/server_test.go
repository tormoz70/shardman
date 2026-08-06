package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/store"
)

func TestHealthz(t *testing.T) {
	srv := &Server{ClusterKey: "test"}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
}

func TestResolveWriteNotBootstrapped(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	dsn := getenv("METADATA_PG_DSN", "")
	if dsn == "" {
		t.Skip("no db")
	}
	ctx := context.Background()
	st, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	srv := &Server{
		Store:      st,
		ClusterKey: "k",
		Resolver:   &resolve.Service{Store: st},
	}
	body, _ := json.Marshal(map[string]string{"shard_key": "2026-08-01"})
	req := httptest.NewRequest(http.MethodPost, "/v1/resolve/write", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		t.Fatal("expected error without bootstrap")
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
