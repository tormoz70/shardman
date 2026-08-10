package oteltrace

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor enriches otelgrpc spans with gRPC status and optional attrs.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			span.SetAttributes(attribute.String("rpc.method", info.FullMethod))
			if err != nil {
				st, _ := status.FromError(err)
				span.SetAttributes(attribute.String("grpc.status_code", st.Code().String()))
				span.SetStatus(codes.Error, st.Message())
			} else {
				span.SetAttributes(attribute.String("grpc.status_code", grpcCodes.OK.String()))
			}
		}
		return resp, err
	}
}

// SetResolveAttrs adds resolve-specific span attributes after a successful resolve.
func SetResolveAttrs(ctx context.Context, bucketID, shardUUID, routing string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("shardman.routing", routing),
	}
	if bucketID != "" {
		attrs = append(attrs, attribute.String("bucket_id", bucketID))
	}
	if shardUUID != "" {
		attrs = append(attrs, attribute.String("shard_uuid", shardUUID))
	}
	span.SetAttributes(attrs...)
}
