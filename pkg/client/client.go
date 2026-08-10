package client

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
	"golang.org/x/sync/errgroup"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/resolve"
)

type Options struct {
	ClusterKey string
	DialOpts   []grpc.DialOption
}

type Client struct {
	addr       string
	clusterKey string
	conn       *grpc.ClientConn
	resolve    shardmanv1.ResolveServiceClient
	topology   shardmanv1.TopologyServiceClient
	admin      shardmanv1.AdminServiceClient

	mu          sync.RWMutex
	topo        *shardmanv1.Topology
	topoVer     int64
	watchCancel context.CancelFunc
}

func Dial(ctx context.Context, addr string, opt Options) (*Client, error) {
	dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opt.DialOpts...)
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr:       addr,
		clusterKey: opt.ClusterKey,
		conn:       conn,
		resolve:    shardmanv1.NewResolveServiceClient(conn),
		topology:   shardmanv1.NewTopologyServiceClient(conn),
		admin:      shardmanv1.NewAdminServiceClient(conn),
	}
	_ = c.refreshTopology(ctx)
	return c, nil
}

func (c *Client) Close() error {
	if c.watchCancel != nil {
		c.watchCancel()
	}
	return c.conn.Close()
}

func (c *Client) adminCtx(ctx context.Context) context.Context {
	if c.clusterKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-cluster-key", c.clusterKey)
}

func (c *Client) refreshTopology(ctx context.Context) error {
	topo, err := c.topology.Get(ctx, &shardmanv1.GetTopologyRequest{})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.topo = topo
	c.topoVer = topo.TopologyVersion
	c.mu.Unlock()
	return nil
}

func (c *Client) WatchTopology(ctx context.Context) error {
	wctx, cancel := context.WithCancel(ctx)
	c.watchCancel = cancel
	stream, err := c.topology.Watch(wctx, &shardmanv1.WatchTopologyRequest{})
	if err != nil {
		cancel()
		return err
	}
	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				cancel()
				return
			}
			c.mu.Lock()
			if ev.Topology != nil {
				c.topo = ev.Topology
				c.topoVer = ev.TopologyVersion
			}
			c.mu.Unlock()
		}
	}()
	return nil
}

func (c *Client) TopologyVersion() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topoVer
}

func (c *Client) ResolveWrite(ctx context.Context, shardKey any) (*resolve.WriteResult, error) {
	if res, ok := c.tryResolveWriteLocal(shardKey); ok {
		return res, nil
	}
	v, err := structpb.NewValue(shardKey)
	if err != nil {
		return nil, err
	}
	res, err := c.resolve.Write(ctx, &shardmanv1.ResolveWriteRequest{ShardKey: v})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			_ = c.refreshTopology(ctx)
		}
		return nil, err
	}
	return &resolve.WriteResult{
		Routing:  bucket.RouteKind(res.GetRouting()),
		BucketID: res.GetBucketId(),
		Reason:   bucket.RouteReason(res.GetReason()),
		ShardID:  res.GetShardId(),
		UUID:     res.GetShardUuid(),
		Endpoint: res.GetEndpoint(),
		State:    fsm.State(res.GetState()),
	}, nil
}

func (c *Client) ResolveRead(ctx context.Context, shardKey any) (*resolve.ReadResult, error) {
	if res, ok := c.tryResolveReadLocal(shardKey); ok {
		return res, nil
	}
	v, err := structpb.NewValue(shardKey)
	if err != nil {
		return nil, err
	}
	res, err := c.resolve.Read(ctx, &shardmanv1.ResolveReadRequest{ShardKey: v})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			_ = c.refreshTopology(ctx)
		}
		return nil, err
	}
	out := &resolve.ReadResult{
		Routing:  bucket.RouteKind(res.GetRouting()),
		BucketID: res.GetBucketId(),
		Reason:   bucket.RouteReason(res.GetReason()),
	}
	for _, sh := range res.GetShards() {
		out.Shards = append(out.Shards, resolve.ReadShard{
			ShardID:  sh.GetShardId(),
			UUID:     sh.GetShardUuid(),
			Endpoint: sh.GetEndpoint(),
			State:    fsm.State(sh.GetState()),
		})
	}
	return out, nil
}

// ScatterQuery fans out a lightweight ping to each active shard endpoint (connectivity check).
func (c *Client) ScatterQuery(ctx context.Context, endpoints []string, fn func(context.Context, string) error) error {
	if fn == nil {
		return errors.New("fn required")
	}
	if len(endpoints) == 0 {
		c.mu.RLock()
		if c.topo != nil {
			for _, sh := range c.topo.Shards {
				if sh.GetRole() == "data" && sh.GetState() == "active" {
					ep := sh.GetAdvertiseUrl()
					if ep == "" {
						ep = sh.GetDsn()
					}
					endpoints = append(endpoints, ep)
				}
			}
		}
		c.mu.RUnlock()
	}
	if len(endpoints) == 0 {
		return errors.New("no endpoints")
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, ep := range endpoints {
		ep := ep
		g.Go(func() error {
			cctx, cancel := context.WithTimeout(gctx, 5*time.Second)
			defer cancel()
			return fn(cctx, ep)
		})
	}
	return g.Wait()
}

func (c *Client) Bootstrap(ctx context.Context, req *shardmanv1.BootstrapRequest) (*shardmanv1.BootstrapResponse, error) {
	resp, err := c.admin.Bootstrap(c.adminCtx(ctx), req)
	if err == nil {
		_ = c.refreshTopology(ctx)
	}
	return resp, err
}

func (c *Client) RegisterShard(ctx context.Context, req *shardmanv1.RegisterShardRequest) (*shardmanv1.Shard, error) {
	resp, err := c.admin.RegisterShard(c.adminCtx(ctx), req)
	if err == nil {
		_ = c.refreshTopology(ctx)
	}
	return resp, err
}

func (c *Client) SealRotate(ctx context.Context, bucketID string) error {
	_, err := c.admin.SealRotate(c.adminCtx(ctx), &shardmanv1.SealRotateRequest{BucketId: bucketID})
	if err == nil {
		_ = c.refreshTopology(ctx)
	}
	return err
}

func (c *Client) GetTopology(ctx context.Context) (*shardmanv1.Topology, error) {
	return c.topology.Get(ctx, &shardmanv1.GetTopologyRequest{})
}

func JSONSpec(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
