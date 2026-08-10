package seal

import (
	"context"
	"log/slog"
	"time"

	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/store"
)

type Supervisor struct {
	Store    *store.Store
	Interval time.Duration
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

func (s *Supervisor) tick(ctx context.Context) {
	cfg, err := s.Store.GetConfig(ctx)
	if err != nil {
		s.Log.Warn("seal: no config", "err", err)
		return
	}
	need, err := s.Store.ShardsNeedingSeal(ctx, cfg.ShardMaxBytes)
	if err != nil {
		s.Log.Warn("seal: list", "err", err)
		return
	}
	for _, sh := range need {
		if sh.BucketID == nil {
			continue
		}
		if err := s.Store.SealRotate(ctx, *sh.BucketID); err != nil {
			s.Log.Warn("seal rotate failed", "bucket", *sh.BucketID, "err", err)
			metrics.IncStandbyExhausted(*sh.BucketID)
			continue
		}
		metrics.IncSeal(*sh.BucketID)
		s.Log.Info("sealed and rotated", "bucket", *sh.BucketID)
	}
	n, _ := s.Store.CountStandbyPool(ctx)
	metrics.SetStandbyPool(n)
}
