# DNS Troubleshooting for Walrus Nodes

When nodes fail with DNS resolution errors, investigate further. All `dig` and `curl` commands
require `dangerouslyDisableSandbox: true`.

## Step 1: Test Multiple Public Resolvers

Determine if the issue is local or widespread:

```bash
for resolver in 8.8.8.8 1.1.1.1 9.9.9.9; do
  echo -n "$resolver: "
  dig +short @$resolver <HOSTNAME>
done
```

## Step 2: Check for Quad9 Blocking

Quad9 (9.9.9.9) uses threat intelligence feeds and may block legitimate domains. If a domain
resolves on Google (8.8.8.8) and Cloudflare (1.1.1.1) but not on Quad9:

- Confirm by testing Quad9's unsecured resolver (no threat blocking):
  ```bash
  dig @9.9.9.10 <HOSTNAME> A +short
  ```
- Query the Quad9 API to find which threat feed is blocking:
  ```bash
  curl -s "https://api.quad9.net/search/<HOSTNAME>"
  ```
  This returns JSON with `blocked`, `blocked_by`, and `meta` fields identifying the threat
  intelligence provider.
- Always provide the user with the Quad9 domain tester link for the affected domain:
  `https://quad9.net/result/?url=<HOSTNAME>#domain-tester`

## Step 3: ChainPatrol False Positives

Web3 infrastructure domains are sometimes flagged by ChainPatrol (a Web3 threat intelligence
provider used by Quad9). If a Walrus node domain is blocked by ChainPatrol:

- Provide the direct ChainPatrol lookup link for the domain:
  `https://app.chainpatrol.io/search?content=<HOSTNAME>`
- Advise the operator to request delisting via that link.

## Step 4: Verify Actual Reachability

Even if DNS fails on some resolvers, the node may still be healthy. Test directly with manual
resolution:

```bash
curl -s --connect-timeout 5 --resolve <HOSTNAME>:9185:<IP> \
  "https://<HOSTNAME>:9185/v1/health"
```
