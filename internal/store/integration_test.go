//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/period"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/store"
)

func TestIntegrationBootstrapResolveSeal(t *testing.T) {
	dsn := os.Getenv("METADATA_PG_DSN")
	if dsn == "" {
		t.Skip("METADATA_PG_DSN not set")
	}
	ctx := context.Background()
	st, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	_, _ = st.Migrate(ctx)

	rd, mf := 3, 0
	spec, _ := json.Marshal(map[string]string{"unit": "month"})
	err = st.Bootstrap(ctx, store.ClusterConfig{
		Mode:             "range",
		PeriodAxis:       period.AxisTime,
		PeriodSpecRaw:    spec,
		ShardMaxBytes:    1024,
		RetentionDepth:   &rd,
		MaxFuturePeriods: &mf,
	})
	if err != nil && err != store.ErrAlreadyBootstrapped {
		t.Fatal(err)
	}

	errUUID := uuid.New()
	dataUUID := uuid.New()
	_, err = st.RegisterShard(ctx, errUUID, fsm.RoleError, "postgres://err/db", "", fsm.StateActive)
	if err != nil && err != store.ErrConflict {
		t.Fatal(err)
	}
	_, err = st.RegisterShard(ctx, dataUUID, fsm.RoleData, "postgres://data/db", "", fsm.StateStandby)
	if err != nil {
		t.Fatal(err)
	}

	svc := &resolve.Service{Store: st}
	res, err := svc.ResolveWrite(ctx, "2099-01-01")
	if err != nil {
		t.Fatal(err)
	}
	if res.Routing != period.RouteError {
		t.Fatalf("expected error route, got %+v", res)
	}
}
