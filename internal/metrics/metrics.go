package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardman_http_requests_total",
		Help: "HTTP requests",
	}, []string{"method", "path", "status"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "shardman_http_request_duration_seconds",
		Help:    "HTTP latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path"})

	shardsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardman_shards",
		Help: "Shard count by bucket and state",
	}, []string{"bucket_id", "state"})

	reportedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardman_shard_reported_bytes",
		Help: "Reported bytes per shard",
	}, []string{"shard_uuid", "bucket_id", "state"})

	sealTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardman_seal_total",
		Help: "Seal events",
	}, []string{"bucket_id"})

	promoteTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardman_promote_total",
		Help: "Promote events",
	}, []string{"bucket_id"})

	standbyPool = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shardman_standby_pool_size",
		Help: "Free standby shards",
	})

	standbyExhausted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardman_standby_exhausted_total",
		Help: "No standby available",
	}, []string{"bucket_id"})

	retentionClean = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardman_retention_clean_total",
		Help: "Retention clean events",
	}, []string{"bucket_id"})

	errorRoute = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "shardman_error_route_total",
		Help: "Writes routed to error shard",
	}, []string{"reason"})

	errorShardBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "shardman_error_shard_bytes",
		Help: "Error shard reported bytes",
	})

	agentLastSeen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shardman_agent_last_seen_seconds",
		Help: "Seconds since agent heartbeat",
	}, []string{"shard_uuid"})
)

func Handler() http.Handler {
	return promhttp.Handler()
}

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(ww, r)
		path := r.URL.Path
		httpRequests.WithLabelValues(r.Method, path, strconv.Itoa(ww.status)).Inc()
		httpDuration.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func IncSeal(bucketID string)        { sealTotal.WithLabelValues(bucketID).Inc() }
func IncPromote(bucketID string)     { promoteTotal.WithLabelValues(bucketID).Inc() }
func IncStandbyExhausted(bucketID string) { standbyExhausted.WithLabelValues(bucketID).Inc() }
func IncRetentionClean(bucketID string)   { retentionClean.WithLabelValues(bucketID).Inc() }
func IncErrorRoute(reason string)    { errorRoute.WithLabelValues(reason).Inc() }
func SetStandbyPool(n int)           { standbyPool.Set(float64(n)) }
func SetErrorShardBytes(n int64)     { errorShardBytes.Set(float64(n)) }
func SetAgentLastSeen(uuid string, secs float64) {
	agentLastSeen.WithLabelValues(uuid).Set(secs)
}

func UpdateShardGauges(bucketID, state, uuid string, bytes int64) {
	shardsGauge.WithLabelValues(bucketID, state).Inc()
	reportedBytes.WithLabelValues(uuid, bucketID, state).Set(float64(bytes))
}

func ResetShardGauges() {
	shardsGauge.Reset()
	reportedBytes.Reset()
}
