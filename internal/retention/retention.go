package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/store"
)

type Supervisor struct {
	Store    *store.Store
	Interval time.Duration
	Now      func() time.Time
	Log      *slog.Logger
}

func (s *Supervisor) Run(ctx context.Context) {
	if s.Interval <= 0 {
		s.Interval = 30 * time.Second
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

func (s *Supervisor) Tick(ctx context.Context) error {
	return s.tick(ctx)
}

func (s *Supervisor) tick(ctx context.Context) error {
	cfg, err := s.Store.GetConfig(ctx)
	if err != nil {
		return err
	}
	if cfg.BucketAxis != bucket.AxisTime || cfg.RetentionDepth == nil {
		return nil
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now()
	}
	known, err := s.Store.DistinctBucketIDs(ctx)
	if err != nil {
		return err
	}
	evict, err := bucket.BucketsOutsideRetention(cfg.BucketSpec, now, *cfg.RetentionDepth, known)
	if err != nil {
		return err
	}
	for _, pid := range evict {
		n, err := s.Store.MarkBucketCleaning(ctx, pid)
		if err != nil {
			s.Log.Warn("retention mark cleaning", "bucket", pid, "err", err)
			continue
		}
		if n > 0 {
			metrics.IncRetentionClean(pid)
			s.Log.Info("retention evict", "bucket", pid, "shards", n)
		}
	}
	return nil
}
