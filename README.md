# SIP Relay

`sip-relay` accepts inbound SIP calls, negotiates PCMU/G.711 mu-law at 8000 Hz, and routes each extension to either Google CES over gRPC or an independent telephony WebSocket service.

The media path intentionally does not decode, resample, mix, or transcode audio. It only parses RTP headers so it can extract and write packet payloads. The one exception is optional, opt-in PCM transcoding on the WebSocket backend (see `websocket.transcode` below), for backends whose audio contract isn't PCMU/8000.

## Run

```sh
go run ./cmd/sip-relay --config config.example.yaml
```

Configure each number under the top-level `extensions` map:

```yaml
ces:
  project_id: project
  location: us
  app_id: app
  endpoint: ces.googleapis.com:443
  credentials_file: /app/credentials.json

websocket:
  base_url: http://telephony-service:8001
  session_timeout: 15s
  connect_timeout: 15s

extensions:
  "1014":
    backend: ces
  "1015":
    backend: websocket
```

CES is gRPC-only. Use application default credentials or set `ces.credentials_file` to a Google service account JSON file. The WebSocket backend does not use CES configuration or Google credentials.
`websocket.base_url` must contain only the HTTP(S) origin; the relay appends the fixed telephony API paths.

## Docker

Build the image:

```sh
docker build -t sip-relay .
```

The runtime image includes `sngrep` for SIP inspection inside containers.

Run with config and credentials mounted:

```sh
docker run --rm \
  -p 5060:5060/tcp \
  -p 5060:5060/udp \
  -p 10000-20000:10000-20000/udp \
  -v "$(pwd)/config.example.yaml:/app/config.yaml:ro" \
  -v "$(pwd)/credentials.json:/app/credentials.json:ro" \
  sip-relay
```

When mounting credentials this way, set `ces.credentials_file` to `/app/credentials.json` in the mounted config.

If your deployment needs explicit memory tuning, pass Go runtime settings such as `GOMEMLIMIT` or `GOGC` with `docker run -e`.

## Call Logs And Recordings

Set `call_log.pubsub_topic_id` and `call_log.pubsub_project_id` to publish a JSON call log when a call ends. The message identifies the selected `backend`, includes optional provider metadata, the conversation ID, ANI, DNIS, timestamps, and hangup reason. Set `call_log.credentials_file` when call logging should use explicit Google credentials.

Set `call_log.recording_bucket` to upload raw `.ulaw` recordings to GCS. Objects are stored as `<backend>/<call_id>.ulaw`.

## SIP/Media Contract

- Incoming SDP must offer `PCMU/8000`.
- CES routes send RTP payload bytes as gRPC `SessionInput.Audio` and packetize `SessionOutput.Audio` as outbound RTP.
- WebSocket routes first POST `/api/v1/telephony/session`, then connect to `/api/v1/telephony/ws/{session_id}`. By default, inbound and outbound audio use binary frames containing raw PCMU bytes; text frames carry lifecycle, transcript, barge-in, and transfer events.
- A `barge_in` event flushes buffered outbound audio. A `transfer` event closes the backend and terminates the SIP dialog with BYE so external telephony infrastructure can perform the human-queue routing.
- Outbound RTP timestamps advance by the number of PCMU payload bytes sent.

### WebSocket PCM transcoding (optional)

If a WebSocket backend expects raw 16-bit linear PCM instead of PCMU, enable transcoding per direction under `websocket.transcode` in the config:

```yaml
websocket:
  transcode:
    input:                # caller -> backend (PCMU 8kHz mu-law -> PCM16LE)
      enabled: true
      sample_rate: 16000
    output:                # backend -> caller (PCM16LE -> PCMU 8kHz mu-law)
      enabled: true
      sample_rate: 24000
```

Both directions default to `enabled: false`, so existing deployments are unaffected unless they opt in. Transcoding happens entirely inside the WebSocket backend package (`internal/audio`, `internal/websocket`); everything else in the relay — RTP playout, recordings, call logs — always sees PCMU/8000 bytes, since that's the only codec the SIP/RTP side ever negotiates. Sample-rate conversion uses a pure-Go, dependency-free resampler (Lagrange interpolation with state kept across chunks, so audio stays smooth across arbitrarily sized WebSocket messages) — no cgo, no change to the container build.