# SIP Relay

`sip-relay` is a separate Go module that accepts inbound SIP calls, negotiates only PCMU/G.711 mu-law at 8000 Hz, forwards RTP payload bytes to Google CES `BidiRunSession`, and wraps CES output bytes back into RTP packets.

The media path intentionally does not decode, resample, mix, or transcode audio. It only parses RTP headers so it can extract and write packet payloads.

## Run

```sh
go run ./cmd/sip-relay --config config.example.yaml
```

Use application default credentials or set `ces.credentials_file` to a Google service account JSON file with access to the CES API.

## Call Logs And Recordings

Set `call_log.pubsub_topic_id` to publish a JSON call log when a call ends. The message contains `call_id`, `ani`, `dnis`, `started_at`, `ended_at`, and `metadata`. For now, `metadata` contains all SIP headers from the inbound `INVITE` as header names mapped to arrays of values. `call_log.pubsub_project_id` is optional and defaults to `ces.project_id`.

Set `call_log.recording_bucket` to upload raw `.ulaw` recordings to GCS. Objects are stored under the CES app ID as `<ces.app_id>/<call_id>.ulaw`.

## SIP/Media Contract

- Incoming SDP must offer `PCMU/8000`.
- Incoming RTP payload bytes are batched into CES `SessionInput.Audio` messages with `AudioEncoding_MULAW`.
- CES `SessionOutput.Audio` bytes are packetized into outbound RTP using the negotiated PCMU payload type.
- Outbound RTP timestamps advance by the number of PCMU payload bytes sent.

