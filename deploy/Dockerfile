FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /server ./cmd/server && \
    CGO_ENABLED=0 go build -o /agent ./cmd/agent && \
    CGO_ENABLED=0 go build -o /shardman ./cmd/shardman

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /server /usr/local/bin/shardman-server
COPY --from=build /agent /usr/local/bin/shardman-agent
COPY --from=build /shardman /usr/local/bin/shardman
EXPOSE 8080 9091
ENTRYPOINT ["/usr/local/bin/shardman-server"]
