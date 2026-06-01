> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Production relayers should emit structured logs, scrape Prometheus metrics, and send alerts for the external systems Walrus Memory depends on: PostgreSQL, Redis, Sui RPC, OpenAI-compatible embedding/LLM APIs, SEAL, Walrus, and the TypeScript sidecar.

## Request correlation

Every relayer request gets an `x-request-id`.

- If the client sends `x-request-id`, the relayer reuses it.
- If the client sends only `x-correlation-id`, the relayer uses that value.
- If neither is present, the relayer generates a UUID.
- The response includes `x-request-id`.
- Rust logs, internal error `traceId` values, outbound sidecar requests, and sidecar error responses use the same ID.

Use this ID when searching logs across the Rust relayer and TypeScript sidecar.

## Logs

For production, run with JSON logs:

```bash
RUST_LOG=memwal_server=info,tower_http=info
LOG_FORMAT=json
```

Useful fields include:

| Field | Meaning |
| --- | --- |
| `request_id` | Request/correlation ID shared across relayer and sidecar |
| `route` | Low-cardinality route label such as `/api/recall` |
| `method` | HTTP method |
| `status` | HTTP status code |
| `latency_ms` | Request latency in milliseconds |
| `owner`, `namespace` | User and namespace fields on selected route logs |

The relayer avoids logging memory text, recall queries, and ask/analyze prompts. It logs byte or character lengths instead.

## Prometheus metrics

The Rust relayer exposes Prometheus metrics at:

```text
GET /metrics
```

The TypeScript sidecar also exposes wallet-specific counters at:

```text
GET <SIDECAR_URL>/metrics/wallet
```

Core relayer metrics:

| Metric | Labels | Notes |
| --- | --- | --- |
| `memwal_http_requests_total` | `method`, `route`, `status` | Request volume and status mix |
| `memwal_http_request_duration_seconds` | `method`, `route`, `status` | HTTP latency histogram |
| `memwal_http_requests_in_flight` | none | Current in-flight request count |
| `memwal_errors_total` | `kind`, `route` | Application error counts |
| `memwal_rate_limit_denials_total` | `bucket`, `route` | Rate-limit denials |
| `memwal_rate_limit_fallbacks_total` | `scope` | Redis fallback usage |
| `memwal_external_request_duration_seconds` | `service`, `operation`, `status` | OpenAI, Sui RPC, SEAL sidecar, Walrus latency |
| `memwal_sidecar_failures_total` | `operation`, `reason` | Sidecar transport and HTTP failures |
| `memwal_db_query_duration_seconds` | `operation`, `status` | PostgreSQL and pgvector query latency |
| `memwal_db_pool_connections` | `state` | PostgreSQL pool `open` and `idle` gauges |

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: memwal-relayer
    metrics_path: /metrics
    static_configs:
      - targets: ["relayer.example.com"]
```

## Recommended dashboards

Create panels for:

- HTTP request rate by route and status.
- p50/p95/p99 HTTP latency by route.
- Error rate from `memwal_errors_total` and 5xx statuses.
- Rate-limit denials by bucket.
- External service p95 latency by `service` and `operation`.
- Sidecar failures by operation.
- PostgreSQL query latency and pool open/idle connections.
- Sidecar wallet counters: `walletSubmittedTotal`, `walletLockErrorsTotal`, `walletPermanentFailuresTotal`.

## Recommended alerts

| Alert | Suggested Condition |
| --- | --- |
| High 5xx rate | 5xx responses exceed 1% for 5 minutes |
| Route latency regression | p95 `/api/recall` or `/api/remember` latency exceeds the normal SLO for 10 minutes |
| Redis degraded | `memwal_rate_limit_fallbacks_total` increases in production |
| Sidecar unhealthy | `memwal_sidecar_failures_total` increases for SEAL or Walrus operations |
| OpenAI latency/errors | External `service="openai"` p95 latency or non-2xx status rate spikes |
| Walrus download/upload failures | External `service="walrus"` or sidecar Walrus failures increase |
| Sui RPC failures | `service="sui_rpc"` transport errors or non-2xx statuses increase |
| DB saturation | PostgreSQL pool open connections near configured max, or idle connections stay at 0 |
| Wallet lock canary | Sidecar `walletLockErrorsTotal` is greater than 0 |
| Permanent wallet failures | Sidecar `walletPermanentFailuresTotal` increases |

## APM integration

Walrus Memory emits structured logs and Prometheus metrics in vendor-neutral formats. For Datadog, New Relic, Grafana Cloud, or OpenTelemetry Collector based setups:

1. Scrape `/metrics` from the Rust relayer.
2. Collect stdout/stderr logs from both the Rust process and sidecar.
3. Parse JSON logs when `LOG_FORMAT=json`.
4. Treat `request_id` and `traceId` as the correlation key.
5. Scrape or poll sidecar `/metrics/wallet` when the sidecar is reachable from the monitoring agent.

If your APM supports custom spans, map `memwal_external_request_duration_seconds` operations to dependencies:

- `openai / embeddings`
- `openai / chat_completions`
- `sui_rpc / sui_getObject`
- `sidecar / seal_encrypt`
- `sidecar / seal_decrypt_batch`
- `sidecar / walrus_upload`
- `walrus / download_blob`