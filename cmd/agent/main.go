package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	shardmanv1 "github.com/tormoz70/shardman/api/gen/shardman/v1"
	"github.com/tormoz70/shardman/internal/agent"
	"github.com/tormoz70/shardman/internal/config"
	"github.com/tormoz70/shardman/internal/oteltrace"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := config.LoadAgent()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	_, _, dialOpts, err := oteltrace.Setup(ctx)
	if err != nil {
		slog.Error("otel", "err", err)
		os.Exit(1)
	}
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(cfg.CoordinatorAddr, dialOpts...)
	if err != nil {
		slog.Error("grpc dial", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := shardmanv1.NewInternalServiceClient(conn)

	ticker := time.NewTicker(cfg.StatsInterval)
	defer ticker.Stop()

	if err := agent.Tick(ctx, cfg, client); err != nil {
		slog.Warn("tick", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := agent.Tick(ctx, cfg, client); err != nil {
				slog.Warn("tick", "err", err)
			}
		}
	}
}
