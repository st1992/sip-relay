FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sip-relay ./cmd/sip-relay

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=builder /out/sip-relay /sip-relay

EXPOSE 5060/tcp
EXPOSE 5060/udp
EXPOSE 10000-20000/udp

USER nonroot:nonroot

ENTRYPOINT ["/sip-relay"]
CMD ["--config", "/etc/sip-relay/config.yaml"]
