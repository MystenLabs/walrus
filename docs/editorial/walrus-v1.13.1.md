---
title: Walrus v1.13.1
description: Reworks TLS configuration by removing the tls.pem_files block in favor of tls.certificate_path and network_key_pair.path, and enables automatic...
keywords: ["walrus", "release notes", "changelog", "testnet", "cli", "tls"]
---

**Testnet** | February 12, 2025

Reworks TLS configuration by removing the `tls.pem_files` block in favor of `tls.certificate_path` and `network_key_pair.path`, and enables automatic rotation of the protocol key pair. Adds a CLI `--ignore-resources` flag.
