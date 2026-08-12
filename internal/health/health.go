package health

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/store"
)

type Supervisor struct {
	Store    *store.Store
	Interval time.Duration
	Log      *slog.Logger
	Notify   func(ctx context.Context)
}

func (s *Supervisor) Run(ctx context.Context) {
	if s.Interval <= 0 {
		s.Interval = 15 * time.Second
	}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Supervisor) Tick(ctx context.Context) {
	s.tick(ctx)
}

func (s *Supervisor) tick(ctx context.Context) {
	stale, err := s.Store.ListStaleActives(ctx)
	if err != nil {
		s.Log.Warn("health: list stale actives", "err", err)
		return
	}
	metrics.SetStaleActiveShards(len(stale))
	for _, sh := range stale {
		if sh.BucketID == nil {
			continue
		}
		bucketID := *sh.BucketID
		start := time.Now()
		if err := s.Store.SealRotate(ctx, bucketID); err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
				continue
			}
			s.Log.Warn("health: seal rotate failed", "bucket", bucketID, "err", err)
			continue
		}
		metrics.ObserveSealDuration(time.Since(start))
		metrics.IncHeartbeatFailover(bucketID)
		s.Log.Info("health: failover stale active", "bucket", bucketID, "shard", sh.ShardUUID)
		if s.Notify != nil {
			s.Notify(ctx)
		}
	}
}
