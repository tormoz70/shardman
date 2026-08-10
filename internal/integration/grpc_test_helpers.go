//go:build integration

package integration

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/grpcapi"
	"google.golang.org/protobuf/types/known/structpb"
)

const bufSize = 1024 * 1024

func bufListener(t *testing.T) *bufconn.Listener {
	t.Helper()
	return bufconn.Listen(bufSize)
}

func dialConn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func startGRPC(t *testing.T, srv *grpcapi.Server) *grpc.ClientConn {
	t.Helper()
	gs := grpc.NewServer()
	shardmanv1.RegisterResolveServiceServer(gs, srv)
	shardmanv1.RegisterAdminServiceServer(gs, srv)
	shardmanv1.RegisterTopologyServiceServer(gs, srv)
	lis := bufListener(t)
	go gs.Serve(lis)
	t.Cleanup(func() { gs.Stop() })
	return dialConn(t, lis)
}

func dialClients(t *testing.T, srv *grpcapi.Server) (shardmanv1.ResolveServiceClient, shardmanv1.AdminServiceClient) {
	conn := startGRPC(t, srv)
	return shardmanv1.NewResolveServiceClient(conn), shardmanv1.NewAdminServiceClient(conn)
}

func dialAdminResolve(t *testing.T, srv *grpcapi.Server) shardmanv1.ResolveServiceClient {
	conn := startGRPC(t, srv)
	return shardmanv1.NewResolveServiceClient(conn)
}

func dialTopology(t *testing.T, srv *grpcapi.Server) shardmanv1.TopologyServiceClient {
	conn := startGRPC(t, srv)
	return shardmanv1.NewTopologyServiceClient(conn)
}

func structpbNew(v any) (*structpb.Value, error) {
	return structpb.NewValue(v)
}
