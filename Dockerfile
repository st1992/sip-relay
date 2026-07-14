# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM golang:1.25-alpine AS build

WORKDIR /src

ENV CGO_ENABLED=0 \
    GOOS=linux

# Cache module downloads separately from the source.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Build the static binary.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
    -ldflags="-s -w" \
    -o /out/sip-relay ./cmd/sip-relay

# ---- Runtime stage ----
# Debian slim keeps debugging tools available, especially sngrep for SIP traces.
FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        sngrep \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system siprelay \
    && useradd --system --gid siprelay --home-dir /app --shell /usr/sbin/nologin siprelay

WORKDIR /app

COPY --from=build /out/sip-relay /usr/local/bin/sip-relay
COPY config.example.yaml /app/config.yaml

EXPOSE 5060/tcp
EXPOSE 5060/udp
EXPOSE 10000-20000/udp

RUN chown -R siprelay:siprelay /app

USER siprelay

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD [ "sh", "-c", "kill -0 1 && grep -q 'sip-relay' /proc/1/cmdline" ]

ENTRYPOINT ["sip-relay"]
CMD ["--config", "/app/config.yaml"]
