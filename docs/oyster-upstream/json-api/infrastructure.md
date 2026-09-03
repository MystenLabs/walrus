# Infrastructure

These endpoints are used for health monitoring and observability. They do
**not** require authentication and are **not** under the `/api/v1/` prefix.

## Health Check (Liveness)

```
GET /health
```

Always returns `200 OK`. Use this as a Kubernetes liveness probe or a
simple "is the server running?" check.

**Example:**

```bash
curl -s "$OYSTER_URL/health" | jq
```

**Response** (`200 OK`):

```json
{
  "status": "ok"
}
```

## Readiness Check

```
GET /ready
```

Returns `200 OK` when all dependencies are healthy, or `503 Service
Unavailable` if any dependency is unreachable. Use this as a Kubernetes
readiness probe to gate traffic until the server is fully operational.

**Example:**

```bash
curl -s "$OYSTER_URL/ready" | jq
```

**Response** (`200 OK`, all healthy):

```json
{
  "ready": true
}
```

**Response** (`503 Service Unavailable`, degraded):

```json
{
  "ready": false,
  "database": "unreachable",
  "pearl": "unreachable"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `ready` | boolean | `true` if all dependencies are healthy |
| `database` | string or absent | `"unreachable"` if the database is down |
| `pearl` | string or absent | `"unreachable"` if the Pearl wallet service is down |

The `database` and `pearl` fields are only present when the corresponding
service is unhealthy.

## Prometheus Metrics

```
GET /metrics
```

Returns metrics in Prometheus text exposition format. Scrape this endpoint
with Prometheus, Grafana Agent, or any compatible collector.

**Example:**

```bash
curl -s "$OYSTER_URL/metrics"
```

**Response** (`200 OK`):

```
# HELP oyster_active_accounts Number of active accounts
# TYPE oyster_active_accounts gauge
oyster_active_accounts 42

# HELP oyster_active_blobs Number of active blobs
# TYPE oyster_active_blobs gauge
oyster_active_blobs 1337

# HELP oyster_blob_store_operations_total Blob store operations
# TYPE oyster_blob_store_operations_total counter
oyster_blob_store_operations_total{operation="store",result="success"} 500
oyster_blob_store_operations_total{operation="read",result="success"} 2000
oyster_blob_store_operations_total{operation="delete",result="success"} 100
```

**Available metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `oyster_active_accounts` | gauge | Total number of accounts |
| `oyster_active_blobs` | gauge | Total number of stored blobs |
| `oyster_blob_store_operations_total` | counter | Blob store operations (labels: `operation`, `result`) |

## OpenAPI Documentation

```
GET /api/docs
```

Serves an interactive API documentation UI (powered by
[Scalar](https://scalar.com/)). Open this URL in your browser to explore
and test all endpoints interactively.

```bash
open "$OYSTER_URL/api/docs"
```
