package bucket

import (
	"fmt"
	"time"
)

type RouteKind string

const (
	RouteBucket RouteKind = "bucket"
	RouteError  RouteKind = "error"
)

type RouteReason string

const (
	ReasonFutureDisallowed RouteReason = "future_disallowed"
	ReasonFutureTooFar     RouteReason = "future_too_far"
	ReasonEvicted          RouteReason = "bucket_evicted"
	ReasonInvalidKey       RouteReason = "invalid_shard_key"
)

type WriteRoute struct {
	Kind     RouteKind   `json:"routing"`
	BucketID string      `json:"bucket_id,omitempty"`
	Reason   RouteReason `json:"reason,omitempty"`
}

// ClassifyWrite decides bucket vs error routing.
func ClassifyWrite(spec Spec, now time.Time, shardKey any, retentionDepth, maxFuture int) (WriteRoute, error) {
	if spec.Axis != AxisTime {
		bid, err := spec.ID(shardKey)
		if err != nil {
			return WriteRoute{Kind: RouteError, BucketID: ErrorBucketID, Reason: ReasonInvalidKey}, nil
		}
		return WriteRoute{Kind: RouteBucket, BucketID: bid}, nil
	}

	targetID, err := spec.ID(shardKey)
	if err != nil {
		return WriteRoute{Kind: RouteError, BucketID: ErrorBucketID, Reason: ReasonInvalidKey}, nil
	}
	currentID, err := spec.CurrentID(now)
	if err != nil {
		return errRoute(err)
	}

	targetIdx, err := spec.Index(targetID)
	if err != nil {
		return WriteRoute{Kind: RouteError, BucketID: ErrorBucketID, Reason: ReasonInvalidKey}, nil
	}
	currentIdx, err := spec.Index(currentID)
	if err != nil {
		return errRoute(err)
	}

	oldestIdx := currentIdx - int64(retentionDepth) + 1
	if targetIdx < oldestIdx {
		return WriteRoute{Kind: RouteError, BucketID: ErrorBucketID, Reason: ReasonEvicted}, nil
	}
	if targetIdx <= currentIdx {
		return WriteRoute{Kind: RouteBucket, BucketID: targetID}, nil
	}
	if maxFuture == 0 {
		return WriteRoute{Kind: RouteError, BucketID: ErrorBucketID, Reason: ReasonFutureDisallowed}, nil
	}
	if targetIdx <= currentIdx+int64(maxFuture) {
		return WriteRoute{Kind: RouteBucket, BucketID: targetID}, nil
	}
	return WriteRoute{Kind: RouteError, BucketID: ErrorBucketID, Reason: ReasonFutureTooFar}, nil
}

// BucketsOutsideRetention returns bucket_ids that should be cleaned (time axis).
func BucketsOutsideRetention(spec Spec, now time.Time, retentionDepth int, known []string) ([]string, error) {
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
	for _, bid := range known {
		if bid == "" || bid == ErrorBucketID {
			continue
		}
		idx, err := spec.Index(bid)
		if err != nil {
			continue
		}
		if idx < oldestIdx {
			out = append(out, bid)
		}
	}
	return out, nil
}

func errRoute(err error) (WriteRoute, error) {
	return WriteRoute{}, err
}

// ClassifyRead returns bucket_id for read or error if outside model.
func ClassifyRead(spec Spec, now time.Time, shardKey any, retentionDepth, maxFuture int) (WriteRoute, error) {
	return ClassifyWrite(spec, now, shardKey, retentionDepth, maxFuture)
}

// ValidateBootstrap checks mode/axis config.
func ValidateBootstrap(mode string, axis Axis, retentionDepth, maxFuture *int) error {
	switch mode {
	case ModeRange, ModeHash, "":
	default:
		return fmt.Errorf("unsupported mode %q", mode)
	}
	if mode == ModeHash || axis == AxisHash {
		if mode != ModeHash && mode != "" {
			return fmt.Errorf("hash axis requires mode=hash")
		}
		if axis != AxisHash {
			return fmt.Errorf("mode=hash requires bucket_axis=hash")
		}
		return nil
	}
	if axis == AxisTime {
		if retentionDepth == nil || *retentionDepth < 1 {
			return fmt.Errorf("retention_depth must be >= 1 for time axis")
		}
		if maxFuture == nil || *maxFuture < 0 {
			return fmt.Errorf("max_future_buckets must be >= 0 for time axis")
		}
	}
	if axis != AxisTime && axis != AxisNumeric {
		return fmt.Errorf("unsupported axis %q for mode=range", axis)
	}
	return nil
}
