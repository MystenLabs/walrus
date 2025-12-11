// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import Layout from '@theme/Layout';

export default function WalrusPDF() {
  return (
    <Layout title="Walrus PDF">
      <div style={{ width: '100%', height: '100vh' }}>
        <iframe
          src="/walrus.pdf"
          style={{ width: '100%', height: '100%', border: 'none' }}
          title="Walrus PDF"
        />
      </div>
    </Layout>
  );
}
