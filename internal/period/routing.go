package period

import (
	"fmt"
	"time"
)

type RouteKind string

const (
	RoutePeriod RouteKind = "period"
	RouteError  RouteKind = "error"
)

type RouteReason string

const (
	ReasonFutureDisallowed RouteReason = "future_disallowed"
	ReasonFutureTooFar     RouteReason = "future_too_far"
	ReasonEvicted          RouteReason = "period_evicted"
	ReasonInvalidKey       RouteReason = "invalid_shard_key"
)

type WriteRoute struct {
	Kind     RouteKind   `json:"routing"`
	PeriodID string      `json:"period_id,omitempty"`
	Reason   RouteReason `json:"reason,omitempty"`
}

// ClassifyWrite decides period vs error routing for time axis.
func ClassifyWrite(spec Spec, now time.Time, shardKey any, retentionDepth, maxFuture int) (WriteRoute, error) {
	if spec.Axis != AxisTime {
		pid, err := spec.ID(shardKey)
		if err != nil {
			return WriteRoute{Kind: RouteError, PeriodID: ErrorPeriodID, Reason: ReasonInvalidKey}, nil
		}
		return WriteRoute{Kind: RoutePeriod, PeriodID: pid}, nil
	}

	targetID, err := spec.ID(shardKey)
	if err != nil {
		return WriteRoute{Kind: RouteError, PeriodID: ErrorPeriodID, Reason: ReasonInvalidKey}, nil
	}
	currentID, err := spec.CurrentID(now)
	if err != nil {
		return errRoute(err)
	}

	targetIdx, err := spec.Index(targetID)
	if err != nil {
		return WriteRoute{Kind: RouteError, PeriodID: ErrorPeriodID, Reason: ReasonInvalidKey}, nil
	}
	currentIdx, err := spec.Index(currentID)
	if err != nil {
		return errRoute(err)
	}

	oldestIdx := currentIdx - int64(retentionDepth) + 1
	if targetIdx < oldestIdx {
		return WriteRoute{Kind: RouteError, PeriodID: ErrorPeriodID, Reason: ReasonEvicted}, nil
	}
	if targetIdx <= currentIdx {
		return WriteRoute{Kind: RoutePeriod, PeriodID: targetID}, nil
	}
	if maxFuture == 0 {
		return WriteRoute{Kind: RouteError, PeriodID: ErrorPeriodID, Reason: ReasonFutureDisallowed}, nil
	}
	if targetIdx <= currentIdx+int64(maxFuture) {
		return WriteRoute{Kind: RoutePeriod, PeriodID: targetID}, nil
	}
	return WriteRoute{Kind: RouteError, PeriodID: ErrorPeriodID, Reason: ReasonFutureTooFar}, nil
}

// PeriodsOutsideRetention returns period_ids that should be cleaned.
func PeriodsOutsideRetention(spec Spec, now time.Time, retentionDepth int, known []string) ([]string, error) {
	if spec.Axis != AxisTime {
		return nil, nil
	}
	currentID, err := spec.CurrentID(now)
	if err != nil {
		return nil, err
	}
	currentIdx, err := spec.Index(currentID)
	if err != nil {
		return nil, err
	}
	oldestIdx := currentIdx - int64(retentionDepth) + 1

	var out []string
	for _, pid := range known {
		if pid == "" || pid == ErrorPeriodID {
			continue
		}
		idx, err := spec.Index(pid)
		if err != nil {
			continue
		}
		if idx < oldestIdx {
			out = append(out, pid)
		}
	}
	return out, nil
}

func errRoute(err error) (WriteRoute, error) {
	return WriteRoute{}, err
}

// ClassifyRead returns period_id for read or error if outside model.
func ClassifyRead(spec Spec, now time.Time, shardKey any, retentionDepth, maxFuture int) (WriteRoute, error) {
	r, err := ClassifyWrite(spec, now, shardKey, retentionDepth, maxFuture)
	if err != nil {
		return r, err
	}
	if r.Kind == RouteError && r.Reason == ReasonFutureDisallowed {
		// future read not in normal periods
		return r, nil
	}
	return r, nil
}

// ValidateBootstrap checks time-axis config.
func ValidateBootstrap(axis Axis, retentionDepth, maxFuture *int) error {
	if axis == AxisTime {
		if retentionDepth == nil || *retentionDepth < 1 {
			return fmt.Errorf("retention_depth must be >= 1 for time axis")
		}
		if maxFuture == nil || *maxFuture < 0 {
			return fmt.Errorf("max_future_periods must be >= 0 for time axis")
		}
	}
	return nil
}
