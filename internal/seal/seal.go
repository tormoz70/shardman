package seal

import (
	"context"
	"log/slog"
	"time"

	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/store"
)

type Supervisor struct {
	Store         *store.Store
	Interval      time.Duration
	DrainTimeout  time.Duration
	Log           *slog.Logger
}

func (s *Supervisor) Run(ctx context.Context) {
	if s.Interval <= 0 {
		s.Interval = 30 * time.Second
	}
	if s.DrainTimeout <= 0 {
		s.DrainTimeout = 30 * time.Second
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

	draining, err := s.Store.ShardsDraining(ctx)
	if err != nil {
		s.Log.Warn("seal: draining list", "err", err)
	} else {
		now := time.Now()
		for _, sh := range draining {
			if sh.BucketID == nil {
				continue
			}
			ready := sh.DrainReady
			if !ready && sh.DrainStartedAt != nil && now.Sub(*sh.DrainStartedAt) >= s.DrainTimeout {
				ready = true
				s.Log.Info("seal: drain timeout elapsed", "bucket", *sh.BucketID)
			}
			if !ready {
				continue
			}
			start := time.Now()
			if err := s.Store.CompleteDrainSeal(ctx, *sh.BucketID); err != nil {
				s.Log.Warn("seal: complete drain failed", "bucket", *sh.BucketID, "err", err)
				metrics.IncStandbyExhausted(*sh.BucketID)
				continue
			}
			metrics.ObserveSealDuration(time.Since(start))
			metrics.IncSeal(*sh.BucketID)
			s.Log.Info("sealed after drain", "bucket", *sh.BucketID)
		}
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
		if err := s.Store.BeginDrain(ctx, *sh.BucketID); err != nil {
			s.Log.Warn("seal: begin drain failed", "bucket", *sh.BucketID, "err", err)
			continue
		}
		s.Log.Info("began drain", "bucket", *sh.BucketID)
	}
	n, _ := s.Store.CountStandbyPool(ctx)
	metrics.SetStandbyPool(n)
}
