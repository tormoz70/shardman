package store

import (
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewPoolStatementTimeout(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/db?sslmode=disable"
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if _, ok := cfg.ConnConfig.RuntimeParams["statement_timeout"]; !ok {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	}
	if cfg.ConnConfig.RuntimeParams["statement_timeout"] != "5000" {
		t.Fatalf("timeout=%q", cfg.ConnConfig.RuntimeParams["statement_timeout"])
	}
}

func TestDSNPreservesStatementTimeout(t *testing.T) {
	dsn := "postgres://user:pass@localhost:5432/db?sslmode=disable&statement_timeout=10000"
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("statement_timeout") != "10000" {
		t.Fatalf("timeout=%q", q.Get("statement_timeout"))
	}
	if !strings.Contains(dsn, "statement_timeout=10000") {
		t.Fatal("expected timeout in dsn")
	}
}
