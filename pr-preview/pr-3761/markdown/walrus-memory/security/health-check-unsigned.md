> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

# Unsigned Health Check Rationale

## Endpoint
`GET /health` and `GET /version`

## Design decision
The health and version endpoints are intentionally left unauthenticated and unsigned. They do not require a valid Ed25519 signature in headers like the rest of the API.

## Security considerations

1. **No Sensitive Information:** 
   The endpoints return service status, package/API versions, SDK compatibility metadata, feature flags, and documented deprecation notices. They do not expose secrets, environment values, uptime, database state, wallet addresses, or credentials.

2. **Load Balancer Integration:**
   Standard load balancers, orchestrators (for example, Kubernetes, Railway), and uptime monitoring tools cannot easily sign requests dynamically. Leaving the endpoint public ensures compatibility with external infrastructure components that rely on straightforward HTTP GET probes.

3. **Rate Limiting:**
Because it is a public unauthenticated route, it bypasses the standard account-based rate limiter middleware. However, because it performs no Database, Redis, LLM, or Walrus operations, the computational path is negligible. If layer 7 DDoS protection is required, it must be handled at the ingress or frontend reverse proxy level.