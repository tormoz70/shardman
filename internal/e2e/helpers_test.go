//go:build e2e

package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/grpcapi"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/seal"
	"github.com/tormoz70/shardman/internal/store"
	"github.com/tormoz70/shardman/internal/topology"
)

func storeNew(ctx context.Context, dsn string) (*store.Store, error) {
	return store.New(ctx, dsn, store.Options{})
}

type testServer struct {
	addr     string
	store    *store.Store
	resolver *resolve.Service
	sealSup  *seal.Supervisor
	retSup   *retention.Supervisor
	stop     func()
}

func startTestServer(ctx context.Context, t *testing.T, metaDSN string) (string, func()) {
	t.Helper()
	ts := startTestServerFull(ctx, t, metaDSN, time.Now)
	return ts.addr, ts.stop
}

func startTestServerFull(ctx context.Context, t *testing.T, metaDSN string, now func() time.Time) *testServer {
	t.Helper()
	st, err := store.New(ctx, metaDSN, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	resolver := &resolve.Service{Store: st, Now: now}
	sealSup := &seal.Supervisor{Store: st, Interval: time.Minute, DrainTimeout: time.Millisecond}
	retSup := &retention.Supervisor{Store: st, Interval: time.Minute, Now: now}
	srv := &grpcapi.Server{
		Store:      st,
		ClusterKey: "e2e-key",
		Resolver:   resolver,
		SealSup:    sealSup,
		RetSup:     retSup,
		Broadcast:  topology.NewBroadcast(),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	grpcapi.Register(gs, srv)
	go gs.Serve(lis)
	return &testServer{
		addr:     lis.Addr().String(),
		store:    st,
		resolver: resolver,
		sealSup:  sealSup,
		retSup:   retSup,
		stop: func() {
			gs.GracefulStop()
			st.Close()
		},
	}
}

var _ = config.Config{}
