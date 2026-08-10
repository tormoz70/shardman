package resolve

import (
	"context"
	"errors"
	"time"

	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/store"
)

type Service struct {
	Store *store.Store
	Now   func() time.Time
}

type WriteResult struct {
	Routing  bucket.RouteKind   `json:"routing"`
	BucketID string             `json:"bucket_id,omitempty"`
	Reason   bucket.RouteReason `json:"reason,omitempty"`
	ShardID  int64              `json:"shard_id"`
	UUID     string             `json:"shard_uuid"`
	Endpoint string             `json:"endpoint"`
	State    fsm.State          `json:"state"`
}

type ReadShard struct {
	ShardID  int64     `json:"shard_id"`
	UUID     string    `json:"shard_uuid"`
	Endpoint string    `json:"endpoint"`
	State    fsm.State `json:"state"`
}

type ReadResult struct {
	Routing  bucket.RouteKind   `json:"routing"`
	BucketID string             `json:"bucket_id,omitempty"`
	Reason   bucket.RouteReason `json:"reason,omitempty"`
	Shards   []ReadShard        `json:"shards"`
}

func (s *Service) ResolveWrite(ctx context.Context, shardKey any) (*WriteResult, error) {
	cfg, err := s.Store.GetConfig(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}

	retention := 0
	maxFuture := 0
	if cfg.RetentionDepth != nil {
		retention = *cfg.RetentionDepth
	}
	if cfg.MaxFutureBuckets != nil {
		maxFuture = *cfg.MaxFutureBuckets
	}

	route, err := bucket.ClassifyWrite(cfg.BucketSpec, now, shardKey, retention, maxFuture)
	if err != nil {
		return nil, err
	}

	if route.Kind == bucket.RouteError {
		metrics.IncErrorRoute(string(route.Reason))
		sh, err := s.Store.GetErrorShard(ctx)
		if err != nil {
			return nil, err
		}
		return &WriteResult{
			Routing:  bucket.RouteError,
			BucketID: bucket.ErrorBucketID,
			Reason:   route.Reason,
			ShardID:  sh.ID,
			UUID:     sh.ShardUUID.String(),
			Endpoint: store.Endpoint(sh),
			State:    sh.State,
		}, nil
	}

	sh, err := s.ensureActive(ctx, route.BucketID)
	if err != nil {
		return nil, err
	}
	return &WriteResult{
		Routing:  bucket.RouteBucket,
		BucketID: route.BucketID,
		ShardID:  sh.ID,
		UUID:     sh.ShardUUID.String(),
		Endpoint: store.Endpoint(sh),
		State:    sh.State,
	}, nil
}

func (s *Service) ResolveRead(ctx context.Context, shardKey any) (*ReadResult, error) {
	cfg, err := s.Store.GetConfig(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}

	retention := 0
	maxFuture := 0
	if cfg.RetentionDepth != nil {
		retention = *cfg.RetentionDepth
	}
	if cfg.MaxFutureBuckets != nil {
		maxFuture = *cfg.MaxFutureBuckets
	}

	route, err := bucket.ClassifyRead(cfg.BucketSpec, now, shardKey, retention, maxFuture)
	if err != nil {
		return nil, err
	}

	if route.Kind == bucket.RouteError {
		if route.Reason == bucket.ReasonEvicted {
			return &ReadResult{Routing: bucket.RouteError, Reason: route.Reason, Shards: nil}, nil
		}
		sh, err := s.Store.GetErrorShard(ctx)
		if err != nil {
			return nil, err
		}
		return &ReadResult{
			Routing:  bucket.RouteError,
			BucketID: bucket.ErrorBucketID,
			Reason:   route.Reason,
			Shards: []ReadShard{{
				ShardID:  sh.ID,
				UUID:     sh.ShardUUID.String(),
				Endpoint: store.Endpoint(sh),
				State:    sh.State,
			}},
		}, nil
	}

	shards, err := s.Store.ShardsForBucketRead(ctx, route.BucketID)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		promoted, err := s.ensureActive(ctx, route.BucketID)
		if err != nil {
			return nil, err
		}
		shards = []store.Shard{*promoted}
	}
	out := make([]ReadShard, 0, len(shards))
	for _, sh := range shards {
		s := sh
		out = append(out, ReadShard{
			ShardID:  s.ID,
			UUID:     s.ShardUUID.String(),
			Endpoint: store.Endpoint(&s),
			State:    s.State,
		})
	}
	return &ReadResult{
		Routing:  bucket.RouteBucket,
		BucketID: route.BucketID,
		Shards:   out,
	}, nil
}

func (s *Service) ensureActive(ctx context.Context, bucketID string) (*store.Shard, error) {
	sh, err := s.Store.ActiveForBucket(ctx, bucketID)
	if err == nil {
		return sh, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if err := s.failoverStaleActive(ctx, bucketID); err != nil {
		return nil, err
	}
	sh, err = s.Store.ActiveForBucket(ctx, bucketID)
	if err == nil {
		return sh, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	promoted, err := s.Store.AutoPromoteIfNoActive(ctx, bucketID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			metrics.IncStandbyExhausted(bucketID)
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	if promoted != nil {
		metrics.IncPromote(bucketID)
		return promoted, nil
	}
	return s.Store.ActiveForBucket(ctx, bucketID)
}

func (s *Service) failoverStaleActive(ctx context.Context, bucketID string) error {
	stale, err := s.Store.StaleActiveForBucket(ctx, bucketID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if stale == nil {
		return nil
	}
	start := time.Now()
	if err := s.Store.SealRotate(ctx, bucketID); err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
			return nil
		}
		return err
	}
	metrics.ObserveSealDuration(time.Since(start))
	metrics.IncHeartbeatFailover(bucketID)
	return nil
}
