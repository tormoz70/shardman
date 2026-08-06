package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tormoz70/shardman/internal/api"
	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/resolve"
	"github.com/tormoz70/shardman/internal/retention"
	"github.com/tormoz70/shardman/internal/seal"
	"github.com/tormoz70/shardman/internal/store"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.LoadServer()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	st, err := store.New(ctx, cfg.MetadataDSN)
	if err != nil {
		slog.Error("store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	resolver := &resolve.Service{Store: st}
	sealSup := &seal.Supervisor{Store: st, Interval: cfg.SealCheckInterval}
	retSup := &retention.Supervisor{Store: st, Interval: cfg.SealCheckInterval}

	srv := &api.Server{
		Store:      st,
		ClusterKey: cfg.ClusterKey,
		Resolver:   resolver,
		SealSup:    sealSup,
		RetSup:     retSup,
	}

	go sealSup.Run(context.Background())
	go retSup.Run(context.Background())
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for range t.C {
			srv.RefreshMetrics(context.Background())
		}
	}()

	httpSrv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: srv.Router(),
	}

	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr)
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
	_ = httpSrv.Shutdown(shutdownCtx)
}
