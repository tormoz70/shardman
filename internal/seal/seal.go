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
		if sh.PeriodID == nil {
			continue
		}
		if err := s.Store.SealRotate(ctx, *sh.PeriodID); err != nil {
			s.Log.Warn("seal rotate failed", "period", *sh.PeriodID, "err", err)
			metrics.IncStandbyExhausted(*sh.PeriodID)
			continue
		}
		metrics.IncSeal(*sh.PeriodID)
		s.Log.Info("sealed and rotated", "period", *sh.PeriodID)
	}
	n, _ := s.Store.CountStandbyPool(ctx)
	metrics.SetStandbyPool(n)
}
