// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import Layout from '@theme/Layout';

export default function WalrusPDF() {
  return (
    <Layout
      title="Walrus Whitepaper"
      description="Read the Walrus protocol whitepaper covering decentralized storage
        architecture, RedStuff erasure coding, and economic design."
    >
      <main>
        <p style={{ padding: '0.75rem 1.5rem', margin: 0 }}>
          The Walrus whitepaper describes the protocol design for decentralized
          blob storage, including the RedStuff erasure coding scheme, storage
          attestation, and token economics.
        </p>
        <div style={{ width: '100%', height: 'calc(100vh - 100px)' }}>
          <iframe
            src="/walrus.pdf"
            style={{ width: '100%', height: '100%', border: 'none' }}
            title="Walrus Whitepaper PDF"
          />
        </div>
      </main>
    </Layout>
  );
}
