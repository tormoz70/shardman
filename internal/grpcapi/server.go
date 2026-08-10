package grpcapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/bucket"
	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/oteltrace"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/seal"
	"github.com/tormoz70/shardman/internal/store"
	"github.com/tormoz70/shardman/internal/topology"
)

const clusterKeyMetadata = "x-cluster-key"

type Server struct {
	shardmanv1.UnimplementedResolveServiceServer
	shardmanv1.UnimplementedTopologyServiceServer
	shardmanv1.UnimplementedAdminServiceServer
	shardmanv1.UnimplementedInternalServiceServer

	Store      *store.Store
	ClusterKey string
	Resolver   *resolve.Service
	SealSup    *seal.Supervisor
	RetSup     *retention.Supervisor
	Broadcast  *topology.Broadcast
	Log        *slog.Logger
}

func Register(s *grpc.Server, srv *Server) {
	shardmanv1.RegisterResolveServiceServer(s, srv)
	shardmanv1.RegisterTopologyServiceServer(s, srv)
	shardmanv1.RegisterAdminServiceServer(s, srv)
	shardmanv1.RegisterInternalServiceServer(s, srv)
}

func (s *Server) notifyTopology(ctx context.Context) {
	if s.Broadcast == nil {
		return
	}
	v, err := s.Store.GetTopologyVersion(ctx)
	if err == nil {
		s.Broadcast.Notify(v)
	}
}

func (s *Server) requireKey(ctx context.Context) error {
	if s.ClusterKey == "" {
		return status.Error(codes.Unavailable, "CLUSTER_KEY not configured")
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get(clusterKeyMetadata)
	if len(vals) == 0 || !checkKey(s.ClusterKey, vals[0]) {
		return status.Error(codes.Unauthenticated, "invalid cluster key")
	}
	return nil
}

func checkKey(expected, got string) bool {
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(got)) == 1
}

func (s *Server) Write(ctx context.Context, req *shardmanv1.ResolveWriteRequest) (*shardmanv1.ResolveWriteResponse, error) {
	start := time.Now()
	defer func() { metrics.ObserveResolve("write", time.Since(start)) }()

	key, err := valueToAny(req.GetShardKey())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "shard_key: %v", err)
	}
	res, err := s.Resolver.ResolveWrite(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.Unavailable, "no active shard or standby pool exhausted")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	oteltrace.SetResolveAttrs(ctx, res.BucketID, res.UUID, string(res.Routing))
	return writeResultToProto(res), nil
}

func (s *Server) Read(ctx context.Context, req *shardmanv1.ResolveReadRequest) (*shardmanv1.ResolveReadResponse, error) {
	start := time.Now()
	defer func() { metrics.ObserveResolve("read", time.Since(start)) }()

	key, err := valueToAny(req.GetShardKey())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "shard_key: %v", err)
	}
	res, err := s.Resolver.ResolveRead(ctx, key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	if len(res.Shards) > 0 {
		oteltrace.SetResolveAttrs(ctx, res.BucketID, res.Shards[0].UUID, string(res.Routing))
	} else {
		oteltrace.SetResolveAttrs(ctx, res.BucketID, "", string(res.Routing))
	}
	return readResultToProto(res), nil
}

func (s *Server) ListBucketShards(ctx context.Context, req *shardmanv1.ListBucketShardsRequest) (*shardmanv1.ListBucketShardsResponse, error) {
	all, err := s.Store.ListShards(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	var filtered []*shardmanv1.Shard
	for _, sh := range all {
		if sh.BucketID != nil && *sh.BucketID == req.GetBucketId() {
			filtered = append(filtered, shardToProto(&sh))
		}
	}
	return &shardmanv1.ListBucketShardsResponse{
		BucketId: req.GetBucketId(),
		Shards:   filtered,
	}, nil
}

func (s *Server) Get(ctx context.Context, _ *shardmanv1.GetTopologyRequest) (*shardmanv1.Topology, error) {
	return s.buildTopology(ctx)
}

func (s *Server) Watch(req *shardmanv1.WatchTopologyRequest, stream shardmanv1.TopologyService_WatchServer) error {
	ctx := stream.Context()
	ch, _, unsub := s.Broadcast.Subscribe()
	defer unsub()

	if req.GetSinceVersion() > 0 {
		topo, err := s.buildTopology(ctx)
		if err != nil {
			return err
		}
		if topo.TopologyVersion > req.GetSinceVersion() {
			if err := stream.Send(&shardmanv1.TopologyEvent{TopologyVersion: topo.TopologyVersion, Topology: topo}); err != nil {
				return err
			}
		}
	} else {
		topo, err := s.buildTopology(ctx)
		if err != nil {
			return err
		}
		if err := stream.Send(&shardmanv1.TopologyEvent{TopologyVersion: topo.TopologyVersion, Topology: topo}); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v := <-ch:
			topo, err := s.buildTopology(ctx)
			if err != nil {
				return err
			}
			if topo.TopologyVersion < v {
				topo.TopologyVersion = v
			}
			if err := stream.Send(&shardmanv1.TopologyEvent{TopologyVersion: topo.TopologyVersion, Topology: topo}); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Bootstrap(ctx context.Context, req *shardmanv1.BootstrapRequest) (*shardmanv1.BootstrapResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	mode := req.GetMode()
	axis := bucket.Axis(req.GetBucketAxis())
	if mode == "" {
		if axis == bucket.AxisHash {
			mode = bucket.ModeHash
		} else {
			mode = bucket.ModeRange
		}
	}
	if mode == bucket.ModeHash && req.GetBucketAxis() == "" {
		axis = bucket.AxisHash
	}
	var rd, mf *int
	if req.RetentionDepth != nil {
		v := int(req.GetRetentionDepth())
		rd = &v
	}
	if req.MaxFutureBuckets != nil {
		v := int(req.GetMaxFutureBuckets())
		mf = &v
	}
	if err := bucket.ValidateBootstrap(mode, axis, rd, mf); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	spec, err := bucket.ParseSpec(axis, req.GetBucketSpecJson())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.GetShardMaxBytes() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "shard_max_bytes must be positive")
	}
	cfg := store.ClusterConfig{
		Mode:             mode,
		BucketAxis:       axis,
		BucketSpec:       spec,
		BucketSpecRaw:    req.GetBucketSpecJson(),
		ShardMaxBytes:    req.GetShardMaxBytes(),
		RetentionDepth:   rd,
		MaxFutureBuckets: mf,
	}
	if err := s.Store.Bootstrap(ctx, cfg); err != nil {
		if errors.Is(err, store.ErrAlreadyBootstrapped) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.notifyTopology(ctx)
	minShards := int32(0)
	if axis == bucket.AxisTime && rd != nil && mf != nil {
		minShards = int32(bucket.MinShards(*rd, *mf))
	}
	return &shardmanv1.BootstrapResponse{Status: "bootstrapped", MinShards: minShards}, nil
}

func (s *Server) ListShards(ctx context.Context, _ *shardmanv1.ListShardsRequest) (*shardmanv1.ListShardsResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	shards, err := s.Store.ListShards(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := make([]*shardmanv1.Shard, 0, len(shards))
	for i := range shards {
		out = append(out, shardToProto(&shards[i]))
	}
	return &shardmanv1.ListShardsResponse{Shards: out}, nil
}

func (s *Server) RegisterShard(ctx context.Context, req *shardmanv1.RegisterShardRequest) (*shardmanv1.Shard, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	u, err := uuid.Parse(req.GetShardUuid())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	role := fsm.RoleData
	if req.GetRole() == "error" {
		role = fsm.RoleError
	}
	sh, err := s.Store.RegisterShard(ctx, u, role, req.GetDsn(), req.GetAdvertiseUrl(), fsm.State(req.GetStartupState()))
	if errors.Is(err, store.ErrConflict) {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	s.notifyTopology(ctx)
	return shardToProto(sh), nil
}

func (s *Server) SealRotate(ctx context.Context, req *shardmanv1.SealRotateRequest) (*shardmanv1.SealRotateResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	if err := s.Store.SealRotate(ctx, req.GetBucketId()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Aborted, err.Error())
	}
	metrics.IncSeal(req.GetBucketId())
	s.notifyTopology(ctx)
	return &shardmanv1.SealRotateResponse{Status: "rotated", BucketId: req.GetBucketId()}, nil
}

func (s *Server) PatchShardState(ctx context.Context, req *shardmanv1.PatchShardStateRequest) (*shardmanv1.PatchShardStateResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	if err := s.Store.PatchState(ctx, req.GetShardId(), fsm.State(req.GetState())); err != nil {
		return nil, status.Error(codes.Aborted, err.Error())
	}
	s.notifyTopology(ctx)
	return &shardmanv1.PatchShardStateResponse{Status: "ok"}, nil
}

func (s *Server) RetentionTick(ctx context.Context, _ *shardmanv1.RetentionTickRequest) (*shardmanv1.RetentionTickResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	if s.RetSup == nil {
		return nil, status.Error(codes.Unavailable, "retention supervisor not configured")
	}
	if err := s.RetSup.Tick(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.notifyTopology(ctx)
	return &shardmanv1.RetentionTickResponse{Status: "ok"}, nil
}

func (s *Server) ReportStats(ctx context.Context, req *shardmanv1.ReportStatsRequest) (*shardmanv1.ReportStatsResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	u, err := uuid.Parse(req.GetShardUuid())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := s.Store.UpdateStats(ctx, u, req.GetReportedBytes()); err != nil {
		metrics.IncAgentStatsError()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	sh, _ := s.Store.GetShardByUUID(ctx, u)
	if sh != nil && sh.Role == fsm.RoleError {
		metrics.SetErrorShardBytes(req.GetReportedBytes())
	}
	metrics.SetAgentLastSeen(req.GetShardUuid(), 0)
	state := ""
	if sh != nil {
		state = string(sh.State)
	}
	return &shardmanv1.ReportStatsResponse{Status: "ok", State: state}, nil
}

func (s *Server) ReportCleaned(ctx context.Context, req *shardmanv1.ReportCleanedRequest) (*shardmanv1.ReportCleanedResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	u, err := uuid.Parse(req.GetShardUuid())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := s.Store.FinishCleaning(ctx, u); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	s.notifyTopology(ctx)
	return &shardmanv1.ReportCleanedResponse{Status: "standby"}, nil
}

func (s *Server) ReportDrainComplete(ctx context.Context, req *shardmanv1.ReportDrainCompleteRequest) (*shardmanv1.ReportDrainCompleteResponse, error) {
	if err := s.requireKey(ctx); err != nil {
		return nil, err
	}
	u, err := uuid.Parse(req.GetShardUuid())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := s.Store.MarkDrainReady(ctx, u); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &shardmanv1.ReportDrainCompleteResponse{Status: "ok"}, nil
}

func (s *Server) RefreshMetrics(ctx context.Context) {
	shards, err := s.Store.ListShards(ctx)
	if err != nil {
		return
	}
	metrics.ResetShardGauges()
	active := 0
	for _, sh := range shards {
		pid := ""
		if sh.BucketID != nil {
			pid = *sh.BucketID
		}
		metrics.UpdateShardGauges(pid, string(sh.State), sh.ShardUUID.String(), sh.ReportedBytes)
		if sh.State == fsm.StateActive {
			active++
		}
		if sh.LastSeenAt != nil {
			metrics.SetAgentLastSeen(sh.ShardUUID.String(), time.Since(*sh.LastSeenAt).Seconds())
		}
	}
	metrics.SetActiveShards(active)
	n, _ := s.Store.CountStandbyPool(ctx)
	metrics.SetStandbyPool(n)
	if s.Broadcast != nil {
		if v, err := s.Store.GetTopologyVersion(ctx); err == nil {
			s.Broadcast.Notify(v)
		}
	}
}

func (s *Server) buildTopology(ctx context.Context) (*shardmanv1.Topology, error) {
	v, err := s.Store.GetTopologyVersion(ctx)
	if err != nil && !errors.Is(err, store.ErrNotBootstrapped) {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	cfg, cfgErr := s.Store.GetConfig(ctx)
	shards, shErr := s.Store.ListShards(ctx)
	if shErr != nil {
		return nil, status.Errorf(codes.Internal, "%v", shErr)
	}
	topo := &shardmanv1.Topology{TopologyVersion: v}
	if cfgErr == nil && cfg != nil {
		var rd, mf *int32
		if cfg.RetentionDepth != nil {
			x := int32(*cfg.RetentionDepth)
			rd = &x
		}
		if cfg.MaxFutureBuckets != nil {
			x := int32(*cfg.MaxFutureBuckets)
			mf = &x
		}
		topo.Config = &shardmanv1.ClusterConfig{
			Mode:             cfg.Mode,
			BucketAxis:       string(cfg.BucketAxis),
			BucketSpecJson:   cfg.BucketSpecRaw,
			ShardMaxBytes:    cfg.ShardMaxBytes,
			RetentionDepth:   rd,
			MaxFutureBuckets: mf,
		}
	}
	for i := range shards {
		topo.Shards = append(topo.Shards, shardToProto(&shards[i]))
	}
	return topo, nil
}

func shardToProto(sh *store.Shard) *shardmanv1.Shard {
	out := &shardmanv1.Shard{
		Id:            sh.ID,
		ShardUuid:     sh.ShardUUID.String(),
		Role:          string(sh.Role),
		State:         string(sh.State),
		Dsn:           sh.DSN,
		ReportedBytes: sh.ReportedBytes,
		Version:       sh.Version,
	}
	if sh.BucketID != nil {
		out.BucketId = *sh.BucketID
	}
	if sh.AdvertiseURL != nil {
		out.AdvertiseUrl = *sh.AdvertiseURL
	}
	if sh.MaxBytes != nil {
		out.MaxBytes = *sh.MaxBytes
	}
	return out
}

func writeResultToProto(res *resolve.WriteResult) *shardmanv1.ResolveWriteResponse {
	return &shardmanv1.ResolveWriteResponse{
		Routing:   string(res.Routing),
		BucketId:  res.BucketID,
		Reason:    string(res.Reason),
		ShardId:   res.ShardID,
		ShardUuid: res.UUID,
		Endpoint:  res.Endpoint,
		State:     string(res.State),
	}
}

func readResultToProto(res *resolve.ReadResult) *shardmanv1.ResolveReadResponse {
	out := &shardmanv1.ResolveReadResponse{
		Routing:  string(res.Routing),
		BucketId: res.BucketID,
		Reason:   string(res.Reason),
	}
	for _, sh := range res.Shards {
		out.Shards = append(out.Shards, &shardmanv1.ReadShard{
			ShardId:   sh.ShardID,
			ShardUuid: sh.UUID,
			Endpoint:  sh.Endpoint,
			State:     string(sh.State),
		})
	}
	return out
}

func valueToAny(v *structpb.Value) (any, error) {
	if v == nil {
		return nil, errors.New("shard_key required")
	}
	return v.AsInterface(), nil
}

func AnyToValue(v any) (*structpb.Value, error) {
	return structpb.NewValue(v)
}
