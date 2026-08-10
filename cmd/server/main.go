package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/grpcapi"
	"github.com/tormoz70/shardman/internal/health"
	"github.com/tormoz70/shardman/internal/metrics"
	"github.com/tormoz70/shardman/internal/opshttp"
	"github.com/tormoz70/shardman/internal/oteltrace"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/seal"
	"github.com/tormoz70/shardman/internal/store"
	"github.com/tormoz70/shardman/internal/topology"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.LoadServer()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	otelShutdown, grpcSrvOpts, _, err := oteltrace.Setup(ctx)
	if err != nil {
		slog.Error("otel", "err", err)
		os.Exit(1)
	}

	st, err := store.New(ctx, cfg.MetadataDSN, store.Options{
		MaxConns:         cfg.MetadataMaxConns,
		HeartbeatTimeout: cfg.HeartbeatTimeout,
	})
	if err != nil {
		slog.Error("store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	resolver := &resolve.Service{Store: st}
	bc := topology.NewBroadcast()
	sealSup := &seal.Supervisor{Store: st, Interval: cfg.SealCheckInterval, DrainTimeout: cfg.DrainTimeout}
	retSup := &retention.Supervisor{Store: st, Interval: cfg.SealCheckInterval}
	healthSup := &health.Supervisor{Store: st, Interval: cfg.HealthCheckInterval}

	srv := &grpcapi.Server{
		Store:      st,
		ClusterKey: cfg.ClusterKey,
		Resolver:   resolver,
		SealSup:    sealSup,
		RetSup:     retSup,
		Broadcast:  bc,
	}
	healthSup.Notify = srv.NotifyTopology

	go sealSup.Run(context.Background())
	go retSup.Run(context.Background())
	go healthSup.Run(context.Background())
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for range t.C {
			srv.RefreshMetrics(context.Background())
		}
	}()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("grpc listen", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer(grpcSrvOpts...)
	grpcapi.Register(grpcSrv, srv)

	go func() {
		slog.Info("grpc listening", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			slog.Error("grpc", "err", err)
			os.Exit(1)
		}
	}()

	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: metrics.Middleware(opshttp.Handler()),
	}
	go func() {
		slog.Info("http ops listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http", "err", err)
			os.Exit(1)
		}
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcSrv.GracefulStop()
	_ = httpSrv.Shutdown(shutdownCtx)
	if otelShutdown != nil {
		_ = otelShutdown(shutdownCtx)
	}
}
