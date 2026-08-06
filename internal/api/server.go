package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/tormoz70/shardman/internal/fsm"
	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/period"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/seal"
	"github.com/tormoz70/shardman/internal/store"
)

type Server struct {
	Store      *store.Store
	ClusterKey string
	Resolver   *resolve.Service
	SealSup    *seal.Supervisor
	RetSup     *retention.Supervisor
	Log        *slog.Logger
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(metrics.Middleware)
	r.Get("/healthz", s.healthz)
	r.Handle("/metrics", metrics.Handler())

	r.Route("/v1", func(r chi.Router) {
		r.Post("/resolve/write", s.resolveWrite)
		r.Post("/resolve/read", s.resolveRead)
		r.Get("/periods/{period_id}/shards", s.periodShards)

		r.Route("/admin", func(r chi.Router) {
			r.Use(s.requireClusterKey)
			r.Post("/bootstrap", s.bootstrap)
			r.Get("/shards", s.listShards)
			r.Post("/shards", s.registerShard)
			r.Post("/seal-rotate", s.sealRotate)
			r.Patch("/shards/{id}/state", s.patchState)
			r.Post("/retention-tick", s.retentionTick)
		})

		r.Route("/internal", func(r chi.Router) {
			r.Use(s.requireClusterKey)
			r.Post("/stats", s.internalStats)
			r.Post("/cleaned", s.internalCleaned)
		})
	})
	return r
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type shardKeyBody struct {
	ShardKey any `json:"shard_key"`
}

func (s *Server) resolveWrite(w http.ResponseWriter, r *http.Request) {
	var body shardKeyBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := s.Resolver.ResolveWrite(r.Context(), body.ShardKey)
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusServiceUnavailable, errors.New("no active shard or standby pool exhausted"))
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) resolveRead(w http.ResponseWriter, r *http.Request) {
	var body shardKeyBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	res, err := s.Resolver.ResolveRead(r.Context(), body.ShardKey)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) periodShards(w http.ResponseWriter, r *http.Request) {
	pid := chi.URLParam(r, "period_id")
	all, err := s.Store.ListShards(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	var filtered []store.Shard
	for _, sh := range all {
		if sh.PeriodID != nil && *sh.PeriodID == pid {
			filtered = append(filtered, sh)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"period_id": pid, "shards": filtered})
}

type bootstrapReq struct {
	Mode             string          `json:"mode"`
	PeriodAxis       string          `json:"period_axis"`
	PeriodSpec       json.RawMessage `json:"period_spec"`
	ShardMaxBytes    int64           `json:"shard_max_bytes"`
	RetentionDepth   *int            `json:"retention_depth"`
	MaxFuturePeriods *int            `json:"max_future_periods"`
}

func (s *Server) bootstrap(w http.ResponseWriter, r *http.Request) {
	var req bootstrapReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.Mode == "" {
		req.Mode = "range"
	}
	axis := period.Axis(req.PeriodAxis)
	if err := period.ValidateBootstrap(axis, req.RetentionDepth, req.MaxFuturePeriods); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	spec, err := period.ParseSpec(axis, req.PeriodSpec)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.ShardMaxBytes <= 0 {
		writeErr(w, http.StatusBadRequest, errors.New("shard_max_bytes must be positive"))
		return
	}
	cfg := store.ClusterConfig{
		Mode:             req.Mode,
		PeriodAxis:       axis,
		PeriodSpec:       spec,
		PeriodSpecRaw:    req.PeriodSpec,
		ShardMaxBytes:    req.ShardMaxBytes,
		RetentionDepth:   req.RetentionDepth,
		MaxFuturePeriods: req.MaxFuturePeriods,
	}
	if err := s.Store.Bootstrap(r.Context(), cfg); err != nil {
		if errors.Is(err, store.ErrAlreadyBootstrapped) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	minShards := 0
	if axis == period.AxisTime && req.RetentionDepth != nil && req.MaxFuturePeriods != nil {
		minShards = period.MinShards(*req.RetentionDepth, *req.MaxFuturePeriods)
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"status":     "bootstrapped",
		"min_shards": minShards,
	})
}

type registerReq struct {
	ShardUUID     string `json:"shard_uuid"`
	Role          string `json:"role"`
	DSN           string `json:"dsn"`
	AdvertiseURL  string `json:"advertise_url"`
	StartupState  string `json:"startup_state"`
	ClusterKey    string `json:"cluster_key"`
}

func (s *Server) registerShard(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	u, err := uuid.Parse(req.ShardUUID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	role := fsm.RoleData
	if req.Role == "error" {
		role = fsm.RoleError
	}
	sh, err := s.Store.RegisterShard(r.Context(), u, role, req.DSN, req.AdvertiseURL, fsm.State(req.StartupState))
	if errors.Is(err, store.ErrConflict) {
		writeErr(w, http.StatusConflict, err)
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, sh)
}

func (s *Server) listShards(w http.ResponseWriter, r *http.Request) {
	shards, err := s.Store.ListShards(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shards": shards})
}

type sealRotateReq struct {
	PeriodID   string `json:"period_id"`
	ClusterKey string `json:"cluster_key"`
}

func (s *Server) sealRotate(w http.ResponseWriter, r *http.Request) {
	var req sealRotateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Store.SealRotate(r.Context(), req.PeriodID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)
			return
		}
		writeErr(w, http.StatusConflict, err)
		return
	}
	metrics.IncSeal(req.PeriodID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "rotated", "period_id": req.PeriodID})
}

func (s *Server) patchState(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var body struct {
		State      string `json:"state"`
		ClusterKey string `json:"cluster_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Store.PatchState(r.Context(), id, fsm.State(body.State)); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) retentionTick(w http.ResponseWriter, r *http.Request) {
	if s.RetSup == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("retention supervisor not configured"))
		return
	}
	if err := s.RetSup.Tick(r.Context()); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type statsReq struct {
	ShardUUID      string `json:"shard_uuid"`
	ReportedBytes  int64  `json:"reported_bytes"`
	ClusterKey     string `json:"cluster_key"`
}

func (s *Server) internalStats(w http.ResponseWriter, r *http.Request) {
	var req statsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	u, err := uuid.Parse(req.ShardUUID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Store.UpdateStats(r.Context(), u, req.ReportedBytes); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	sh, _ := s.Store.GetShardByUUID(r.Context(), u)
	if sh != nil && sh.Role == fsm.RoleError {
		metrics.SetErrorShardBytes(req.ReportedBytes)
	}
	metrics.SetAgentLastSeen(req.ShardUUID, 0)
	state := ""
	if sh != nil {
		state = string(sh.State)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "state": state})
}

type cleanedReq struct {
	ShardUUID  string `json:"shard_uuid"`
	ClusterKey string `json:"cluster_key"`
}

func (s *Server) internalCleaned(w http.ResponseWriter, r *http.Request) {
	var req cleanedReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	u, err := uuid.Parse(req.ShardUUID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.Store.FinishCleaning(r.Context(), u); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "standby"})
}

func (s *Server) requireClusterKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.ClusterKey == "" {
			writeErr(w, http.StatusServiceUnavailable, errors.New("CLUSTER_KEY not configured"))
			return
		}
		if !checkKey(s.ClusterKey, r.Header.Get("X-Cluster-Key")) {
			writeErr(w, http.StatusUnauthorized, errors.New("invalid cluster key"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func checkKey(expected, got string) bool {
	if expected == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(got)) == 1
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Server) RefreshMetrics(ctx context.Context) {
	shards, err := s.Store.ListShards(ctx)
	if err != nil {
		return
	}
	metrics.ResetShardGauges()
	for _, sh := range shards {
		pid := ""
		if sh.PeriodID != nil {
			pid = *sh.PeriodID
		}
		metrics.UpdateShardGauges(pid, string(sh.State), sh.ShardUUID.String(), sh.ReportedBytes)
		if sh.LastSeenAt != nil {
			metrics.SetAgentLastSeen(sh.ShardUUID.String(), time.Since(*sh.LastSeenAt).Seconds())
		}
	}
	n, _ := s.Store.CountStandbyPool(ctx)
	metrics.SetStandbyPool(n)
}
