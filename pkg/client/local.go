package client

import (
	"errors"
	"time"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/resolve"
)

type localClusterConfig struct {
	BucketSpec       bucket.Spec
	RetentionDepth   *int
	MaxFutureBuckets *int
}

func clusterConfigFromProto(cfg *shardmanv1.ClusterConfig) (*localClusterConfig, error) {
	if cfg == nil {
		return nil, errors.New("cluster config required")
	}
	axis := bucket.Axis(cfg.GetBucketAxis())
	spec, err := bucket.ParseSpec(axis, cfg.GetBucketSpecJson())
	if err != nil {
		return nil, err
	}
	out := &localClusterConfig{BucketSpec: spec}
	if cfg.RetentionDepth != nil {
		v := int(cfg.GetRetentionDepth())
		out.RetentionDepth = &v
	}
	if cfg.MaxFutureBuckets != nil {
		v := int(cfg.GetMaxFutureBuckets())
		out.MaxFutureBuckets = &v
	}
	return out, nil
}

func retentionArgs(cfg *localClusterConfig) (retention, maxFuture int) {
	if cfg.RetentionDepth != nil {
		retention = *cfg.RetentionDepth
	}
	if cfg.MaxFutureBuckets != nil {
		maxFuture = *cfg.MaxFutureBuckets
	}
	return retention, maxFuture
}

func shardEndpoint(sh *shardmanv1.Shard) string {
	if sh.GetAdvertiseUrl() != "" {
		return sh.GetAdvertiseUrl()
	}
	return sh.GetDsn()
}

func findErrorShard(shards []*shardmanv1.Shard) *shardmanv1.Shard {
	for _, sh := range shards {
		if sh.GetRole() == string(fsm.RoleError) {
			return sh
		}
	}
	return nil
}

func findActiveForBucket(shards []*shardmanv1.Shard, bucketID string) *shardmanv1.Shard {
	for _, sh := range shards {
		if sh.GetRole() == string(fsm.RoleData) && sh.GetState() == string(fsm.StateActive) && sh.GetBucketId() == bucketID {
			return sh
		}
	}
	return nil
}

func shardsForBucketRead(shards []*shardmanv1.Shard, bucketID string) []*shardmanv1.Shard {
	var out []*shardmanv1.Shard
	for _, sh := range shards {
		if sh.GetRole() != string(fsm.RoleData) || sh.GetBucketId() != bucketID {
			continue
		}
		switch sh.GetState() {
		case string(fsm.StateActive), string(fsm.StateSealed):
			out = append(out, sh)
		}
	}
	return out
}

func (c *Client) tryResolveWriteLocal(shardKey any) (*resolve.WriteResult, bool) {
	c.mu.RLock()
	topo := c.topo
	c.mu.RUnlock()
	if topo == nil || topo.Config == nil {
		return nil, false
	}
	cfg, err := clusterConfigFromProto(topo.Config)
	if err != nil {
		return nil, false
	}
	retention, maxFuture := retentionArgs(cfg)
	route, err := bucket.ClassifyWrite(cfg.BucketSpec, time.Now(), shardKey, retention, maxFuture)
	if err != nil {
		return nil, false
	}
	if route.Kind == bucket.RouteError {
		sh := findErrorShard(topo.Shards)
		if sh == nil {
			return nil, false
		}
		return &resolve.WriteResult{
			Routing:  bucket.RouteError,
			BucketID: bucket.ErrorBucketID,
			Reason:   route.Reason,
			ShardID:  sh.GetId(),
			UUID:     sh.GetShardUuid(),
			Endpoint: shardEndpoint(sh),
			State:    fsm.State(sh.GetState()),
		}, true
	}
	sh := findActiveForBucket(topo.Shards, route.BucketID)
	if sh == nil {
		return nil, false
	}
	return &resolve.WriteResult{
		Routing:  bucket.RouteBucket,
		BucketID: route.BucketID,
		ShardID:  sh.GetId(),
		UUID:     sh.GetShardUuid(),
		Endpoint: shardEndpoint(sh),
		State:    fsm.State(sh.GetState()),
	}, true
}

func (c *Client) tryResolveReadLocal(shardKey any) (*resolve.ReadResult, bool) {
	c.mu.RLock()
	topo := c.topo
	c.mu.RUnlock()
	if topo == nil || topo.Config == nil {
		return nil, false
	}
	cfg, err := clusterConfigFromProto(topo.Config)
	if err != nil {
		return nil, false
	}
	retention, maxFuture := retentionArgs(cfg)
	route, err := bucket.ClassifyRead(cfg.BucketSpec, time.Now(), shardKey, retention, maxFuture)
	if err != nil {
		return nil, false
	}
	if route.Kind == bucket.RouteError {
		if route.Reason == bucket.ReasonEvicted {
			return &resolve.ReadResult{
				Routing: bucket.RouteError,
				Reason:  route.Reason,
				Shards:  nil,
			}, true
		}
		sh := findErrorShard(topo.Shards)
		if sh == nil {
			return nil, false
		}
		return &resolve.ReadResult{
			Routing:  bucket.RouteError,
			BucketID: bucket.ErrorBucketID,
			Reason:   route.Reason,
			Shards: []resolve.ReadShard{{
				ShardID:  sh.GetId(),
				UUID:     sh.GetShardUuid(),
				Endpoint: shardEndpoint(sh),
				State:    fsm.State(sh.GetState()),
			}},
		}, true
	}
	matches := shardsForBucketRead(topo.Shards, route.BucketID)
	if len(matches) == 0 {
		return nil, false
	}
	out := &resolve.ReadResult{
		Routing:  bucket.RouteBucket,
		BucketID: route.BucketID,
		Shards:   make([]resolve.ReadShard, 0, len(matches)),
	}
	for _, sh := range matches {
		out.Shards = append(out.Shards, resolve.ReadShard{
			ShardID:  sh.GetId(),
			UUID:     sh.GetShardUuid(),
			Endpoint: shardEndpoint(sh),
			State:    fsm.State(sh.GetState()),
		})
	}
	return out, true
}
