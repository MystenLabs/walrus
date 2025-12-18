// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import Layout from "@theme/Layout";
import Head from "@docusaurus/Head";
import Link from "@docusaurus/Link";
import styles from "./index.module.css";

export default function Home() {
  return (
    <>
      <Head>
        <meta
          name="description"
          content="Developer documentation for Walrus, a decentralized storage and data availability protocol designed for large binary files and blobs."
        />
      </Head>
      <Layout>
        <main className={styles.main}>
          {/* Hero */}
          <div className={styles.hero}>
            <h1 className={styles.title}>Walrus Documentation</h1>
            <p className={styles.subtitle}>
              Walrus is a decentralized storage protocol designed specifically to enable data markets for the AI era and make data storage reliable, valuable, and governable.
            </p>
          </div>

          <div className={styles.container}>
            {/* Features */}
            <div className={styles.features}>
              <div className={styles.feature}>
                <h3>Cost Efficient</h3>
                <p>
                  Advanced erasure coding maintains storage at ~5x blob size while ensuring high availability.
                </p>
              </div>
              <div className={styles.feature}>
                <h3>Byzantine Fault Tolerant</h3>
                <p>
                  Data remains accessible even when storage nodes are unavailable or malicious.
                </p>
              </div>
              <div className={styles.feature}>
                <h3>Integrated with Sui</h3>
                <p>
                  Storage space and stored blobs are represented as Sui objects that can be owned, transferred, and managed by smart contracts.
                </p>
              </div>
              <div className={styles.feature}>
                <h3>Flexible Access</h3>
                <p>
                  Interact via CLI, SDKs, or HTTP APIs with support for traditional CDNs.
                </p>
              </div>
            </div>

            {/* Cards Grid */}
            <div className={styles.grid}>
              <Link to="/docs/usage/started" className={styles.card}>
                <h3>Get Started</h3>
                <p>Install the Walrus CLI and configure your environment</p>
              </Link>

              <Link to="/docs/usage/client-cli" className={styles.card}>
                <h3>Store and Retrieve</h3>
                <p>Learn how to store blobs and read data using the CLI</p>
              </Link>

              <Link to="/docs/usage/web-api" className={styles.card}>
                <h3>HTTP API</h3>
                <p>Use aggregators and publishers via REST endpoints</p>
              </Link>

              <Link to="/docs/walrus-sites/intro" className={styles.card}>
                <h3>Walrus Sites</h3>
                <p>Build and deploy decentralized websites on Walrus</p>
              </Link>

              <Link to="/docs/design/architecture" className={styles.card}>
                <h3>Architecture</h3>
                <p>Understand the design principles and system architecture</p>
              </Link>

              <Link to="/docs/usage/sdks" className={styles.card}>
                <h3>Developer Guide</h3>
                <p>Integrate Walrus into your applications with SDKs</p>
              </Link>
            </div>
          </div>
        </main>
      </Layout>
    </>
  );
}