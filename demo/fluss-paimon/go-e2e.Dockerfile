FROM golang:1.25 AS builder

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/go-e2e ./demo/fluss-paimon/go-e2e

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/go-e2e /usr/local/bin/go-e2e
ENTRYPOINT ["/usr/local/bin/go-e2e"]
