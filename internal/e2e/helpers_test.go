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
	return store.New(ctx, dsn)
}

func startTestServer(ctx context.Context, t *testing.T, metaDSN string) (string, func()) {
	t.Helper()
	st, err := store.New(ctx, metaDSN)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &resolve.Service{Store: st}
	srv := &grpcapi.Server{
		Store:      st,
		ClusterKey: "e2e-key",
		Resolver:   resolver,
		SealSup:    &seal.Supervisor{Store: st, Interval: time.Minute},
		RetSup:     &retention.Supervisor{Store: st, Interval: time.Minute},
		Broadcast:  topology.NewBroadcast(),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	grpcapi.Register(gs, srv)
	go gs.Serve(lis)
	return lis.Addr().String(), func() {
		gs.GracefulStop()
		st.Close()
	}
}

var _ = config.Config{}
