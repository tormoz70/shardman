package resolve

import (
	"context"
	"errors"
	"time"

	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/period"
	"github.com/tormoz70/shardman/internal/store"
)

type Service struct {
	Store *store.Store
	Now   func() time.Time
}

type WriteResult struct {
	Routing  period.RouteKind   `json:"routing"`
	PeriodID string             `json:"period_id,omitempty"`
	Reason   period.RouteReason `json:"reason,omitempty"`
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
	Routing  period.RouteKind   `json:"routing"`
	PeriodID string             `json:"period_id,omitempty"`
	Reason   period.RouteReason `json:"reason,omitempty"`
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
	if cfg.MaxFuturePeriods != nil {
		maxFuture = *cfg.MaxFuturePeriods
	}

	route, err := period.ClassifyWrite(cfg.PeriodSpec, now, shardKey, retention, maxFuture)
	if err != nil {
		return nil, err
	}

	if route.Kind == period.RouteError {
		metrics.IncErrorRoute(string(route.Reason))
		sh, err := s.Store.GetErrorShard(ctx)
		if err != nil {
			return nil, err
		}
		return &WriteResult{
			Routing:  period.RouteError,
			PeriodID: period.ErrorPeriodID,
			Reason:   route.Reason,
			ShardID:  sh.ID,
			UUID:     sh.ShardUUID.String(),
			Endpoint: store.Endpoint(sh),
			State:    sh.State,
		}, nil
	}

	sh, err := s.ensureActive(ctx, route.PeriodID)
	if err != nil {
		return nil, err
	}
	return &WriteResult{
		Routing:  period.RoutePeriod,
		PeriodID: route.PeriodID,
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
	if cfg.MaxFuturePeriods != nil {
		maxFuture = *cfg.MaxFuturePeriods
	}

	route, err := period.ClassifyRead(cfg.PeriodSpec, now, shardKey, retention, maxFuture)
	if err != nil {
		return nil, err
	}

	if route.Kind == period.RouteError {
		if route.Reason == period.ReasonEvicted {
			return &ReadResult{Routing: period.RouteError, Reason: route.Reason, Shards: nil}, nil
		}
		sh, err := s.Store.GetErrorShard(ctx)
		if err != nil {
			return nil, err
		}
		return &ReadResult{
			Routing:  period.RouteError,
			PeriodID: period.ErrorPeriodID,
			Reason:   route.Reason,
			Shards: []ReadShard{{
				ShardID:  sh.ID,
				UUID:     sh.ShardUUID.String(),
				Endpoint: store.Endpoint(sh),
				State:    sh.State,
			}},
		}, nil
	}

	shards, err := s.Store.ShardsForPeriodRead(ctx, route.PeriodID)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		promoted, err := s.ensureActive(ctx, route.PeriodID)
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
		Routing:  period.RoutePeriod,
		PeriodID: route.PeriodID,
		Shards:   out,
	}, nil
}

func (s *Service) ensureActive(ctx context.Context, periodID string) (*store.Shard, error) {
	sh, err := s.Store.ActiveForPeriod(ctx, periodID)
	if err == nil {
		return sh, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	promoted, err := s.Store.AutoPromoteIfNoActive(ctx, periodID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			metrics.IncStandbyExhausted(periodID)
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	if promoted != nil {
		metrics.IncPromote(periodID)
		return promoted, nil
	}
	return s.Store.ActiveForPeriod(ctx, periodID)
}
