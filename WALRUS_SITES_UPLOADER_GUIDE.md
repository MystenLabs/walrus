# Walrus Sites Blob Uploader - Complete Implementation Guide

This guide provides a complete architecture and implementation plan for building
a Walrus Sites UI that allows users to upload and list blobs using Slush
wallet integration.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Implementation Details](#implementation-details)
5. [Deployment](#deployment)
6. [Important Considerations](#important-considerations)

---

## Architecture Overview

The application consists of four main components:

1. **React SPA Frontend** - Hosted as a Walrus Site (decentralized static website)
2. **Slush Wallet Integration** - For user authentication and transaction signing
3. **Walrus SDK** - For blob upload/download operations
4. **Sui Blockchain** - For metadata and ownership management

### How It Works

```
┌─────────────────────────────────────────────────────────────┐
│                    Walrus Site (React SPA)                   │
│  ┌────────────────┐  ┌──────────────┐  ┌─────────────────┐ │
│  │ Wallet Connect │  │ File Upload  │  │   Blob List     │ │
│  └────────────────┘  └──────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                    ↓                 ↓                 ↓
         ┌──────────────────┐  ┌──────────────────────────┐
         │   Slush Wallet   │  │    Walrus Protocol       │
         │  (Authentication)│  │  (Blob Storage Nodes)    │
         └──────────────────┘  └──────────────────────────┘
                    ↓
         ┌──────────────────┐
         │  Sui Blockchain  │
         │   (Metadata)     │
         └──────────────────┘
```

---

## Prerequisites

### Required Tools

1. **Node.js & npm** (v18 or higher)
2. **Rust** (for building from source, optional)
3. **Sui Wallet** with SUI and WAL tokens
   - SUI for transaction gas fees
   - WAL for storage fees

### Required Knowledge

- React and TypeScript basics
- Understanding of Web3 wallets
- Basic command line usage

---

## Setup Instructions

### 1. Create React App with Vite

```bash
# Create new Vite project
npm create vite@latest walrus-uploader -- --template react-ts
cd walrus-uploader

# Install core dependencies
npm install @mysten/dapp-kit @mysten/sui @mysten/walrus @tanstack/react-query

# Install dApp Kit styles
npm install
```

### 2. Install Walrus Site Builder

```bash
# Install site-builder tool
curl -sSf https://install.wal.app | sh

# Download configuration file
mkdir -p ~/.config/walrus
curl https://raw.githubusercontent.com/MystenLabs/walrus-sites/refs/heads/mainnet/sites-config.yaml -o ~/.config/walrus/sites-config.yaml

# Verify installation
site-builder --help
```

---

## Implementation Details

### Project Structure

```
walrus-uploader/
├── src/
│   ├── components/
│   │   ├── WalletConnection.tsx
│   │   ├── FileUpload.tsx
│   │   └── BlobList.tsx
│   ├── lib/
│   │   └── walrus.ts
│   ├── App.tsx
│   └── main.tsx
├── public/
├── ws-resources.json
├── package.json
└── vite.config.ts
```

### 1. Configure Vite for WASM Support

```typescript
// vite.config.ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    exclude: ['@mysten/walrus-wasm']
  }
})
```

### 2. App Setup with Providers

```typescript
// src/App.tsx
import { createNetworkConfig, SuiClientProvider, WalletProvider } from '@mysten/dapp-kit';
import { getFullnodeUrl } from '@mysten/sui/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import '@mysten/dapp-kit/dist/index.css';
import { WalletConnection } from './components/WalletConnection';
import { FileUpload } from './components/FileUpload';
import { BlobList } from './components/BlobList';

// Configure network
const { networkConfig } = createNetworkConfig({
  mainnet: { url: getFullnodeUrl('mainnet') },
  testnet: { url: getFullnodeUrl('testnet') }
});

const queryClient = new QueryClient();

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <SuiClientProvider networks={networkConfig} defaultNetwork="mainnet">
        <WalletProvider
          slushWallet={{
            name: 'Walrus Blob Uploader',
          }}
        >
          <div className="app">
            <header>
              <h1>Walrus Blob Uploader</h1>
              <WalletConnection />
            </header>
            <main>
              <FileUpload />
              <BlobList />
            </main>
          </div>
        </WalletProvider>
      </SuiClientProvider>
    </QueryClientProvider>
  );
}

export default App;
```

### 3. Walrus Client Setup

```typescript
// src/lib/walrus.ts
import { SuiClient } from '@mysten/sui/client';
import { WalrusClient } from '@mysten/walrus';
import walrusWasmUrl from '@mysten/walrus-wasm/web/walrus_wasm_bg.wasm?url';

export const createWalrusClient = (suiClient: SuiClient, network: 'mainnet' | 'testnet' = 'mainnet') => {
  return new WalrusClient({
    network,
    suiClient,
    wasmUrl: walrusWasmUrl, // Required for Vite bundler
  });
};
```

### 4. Wallet Connection Component

```typescript
// src/components/WalletConnection.tsx
import { ConnectButton, useCurrentAccount } from '@mysten/dapp-kit';

export function WalletConnection() {
  const account = useCurrentAccount();

  return (
    <div className="wallet-connection">
      <ConnectButton />
      {account && (
        <div className="account-info">
          <p>Connected: {account.address.slice(0, 6)}...{account.address.slice(-4)}</p>
        </div>
      )}
    </div>
  );
}
```

### 5. File Upload Component

```typescript
// src/components/FileUpload.tsx
import { useCurrentAccount, useSuiClient } from '@mysten/dapp-kit';
import { WalrusFile } from '@mysten/walrus';
import { useState } from 'react';
import { createWalrusClient } from '../lib/walrus';

interface UploadResult {
  blobId: string;
  fileName: string;
  size: number;
  timestamp: number;
}

export function FileUpload() {
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [uploadHistory, setUploadHistory] = useState<UploadResult[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [epochs, setEpochs] = useState(5);

  const currentAccount = useCurrentAccount();
  const suiClient = useSuiClient();

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0];
    if (selectedFile) {
      setFile(selectedFile);
      setError(null);
    }
  };

  const handleUpload = async () => {
    if (!file || !currentAccount) {
      setError('Please select a file and connect your wallet');
      return;
    }

    setUploading(true);
    setError(null);

    try {
      const walrusClient = createWalrusClient(suiClient);

      // Convert File to WalrusFile
      const arrayBuffer = await file.arrayBuffer();
      const walrusFile = WalrusFile.from({
        contents: new Uint8Array(arrayBuffer),
        identifier: file.name,
      });

      console.log(`Uploading ${file.name} (${file.size} bytes) for ${epochs} epochs...`);

      // Upload to Walrus - this will prompt user to sign transaction via Slush wallet
      const results = await walrusClient.writeFiles({
        files: [walrusFile],
        epochs: epochs,
        deletable: true,
        signer: currentAccount, // Uses connected Slush wallet
      });

      // Get the result
      const result = await results.next();
      if (result.value) {
        const uploadResult: UploadResult = {
          blobId: result.value.blobId,
          fileName: file.name,
          size: file.size,
          timestamp: Date.now(),
        };

        setUploadHistory(prev => [uploadResult, ...prev]);
        console.log('Upload successful:', result.value);

        // Save to localStorage for persistence
        const saved = localStorage.getItem('walrus-uploads');
        const existing = saved ? JSON.parse(saved) : [];
        localStorage.setItem('walrus-uploads', JSON.stringify([uploadResult, ...existing]));

        // Reset form
        setFile(null);
        if (document.querySelector('input[type="file"]')) {
          (document.querySelector('input[type="file"]') as HTMLInputElement).value = '';
        }
      }
    } catch (error) {
      console.error('Upload failed:', error);
      setError(error instanceof Error ? error.message : 'Upload failed');
    } finally {
      setUploading(false);
    }
  };

  return (
    <div className="file-upload">
      <h2>Upload File to Walrus</h2>

      <div className="upload-form">
        <input
          type="file"
          onChange={handleFileChange}
          disabled={!currentAccount || uploading}
        />

        <div className="epochs-selector">
          <label>
            Storage Duration (Epochs):
            <input
              type="number"
              min="1"
              max="100"
              value={epochs}
              onChange={(e) => setEpochs(parseInt(e.target.value))}
              disabled={uploading}
            />
          </label>
          <small>1 epoch ≈ 2 weeks on Mainnet, 1 day on Testnet</small>
        </div>

        <button
          onClick={handleUpload}
          disabled={!file || !currentAccount || uploading}
        >
          {uploading ? 'Uploading...' : 'Upload to Walrus'}
        </button>
      </div>

      {error && <div className="error">{error}</div>}

      {file && (
        <div className="file-info">
          <p>Selected: {file.name} ({(file.size / 1024).toFixed(2)} KB)</p>
        </div>
      )}

      {uploadHistory.length > 0 && (
        <div className="upload-history">
          <h3>Recent Uploads</h3>
          {uploadHistory.map((upload, idx) => (
            <div key={idx} className="upload-item">
              <p><strong>File:</strong> {upload.fileName}</p>
              <p><strong>Blob ID:</strong> <code>{upload.blobId}</code></p>
              <p><strong>Size:</strong> {(upload.size / 1024).toFixed(2)} KB</p>
              <p><strong>Uploaded:</strong> {new Date(upload.timestamp).toLocaleString()}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

### 6. Blob List Component

```typescript
// src/components/BlobList.tsx
import { useSuiClient } from '@mysten/dapp-kit';
import { useEffect, useState } from 'react';
import { createWalrusClient } from '../lib/walrus';

interface BlobInfo {
  id: string;
  blob?: Blob;
  error?: string;
  loading: boolean;
}

export function BlobList() {
  const [blobIds, setBlobIds] = useState<string[]>([]);
  const [blobs, setBlobs] = useState<Map<string, BlobInfo>>(new Map());
  const [inputBlobId, setInputBlobId] = useState('');
  const suiClient = useSuiClient();

  // Load blob IDs from localStorage on mount
  useEffect(() => {
    const saved = localStorage.getItem('walrus-uploads');
    if (saved) {
      const uploads = JSON.parse(saved);
      const ids = uploads.map((u: any) => u.blobId);
      setBlobIds(ids);
    }
  }, []);

  const loadBlob = async (blobId: string) => {
    setBlobs(prev => new Map(prev).set(blobId, { id: blobId, loading: true }));

    try {
      const walrusClient = createWalrusClient(suiClient);
      const [file] = await walrusClient.getFiles({ ids: [blobId] });
      const blob = await file.blob();

      setBlobs(prev => new Map(prev).set(blobId, {
        id: blobId,
        blob,
        loading: false
      }));
    } catch (error) {
      console.error(`Failed to load blob ${blobId}:`, error);
      setBlobs(prev => new Map(prev).set(blobId, {
        id: blobId,
        error: error instanceof Error ? error.message : 'Failed to load',
        loading: false
      }));
    }
  };

  const handleAddBlob = () => {
    if (inputBlobId && !blobIds.includes(inputBlobId)) {
      setBlobIds(prev => [...prev, inputBlobId]);
      loadBlob(inputBlobId);
      setInputBlobId('');
    }
  };

  const downloadBlob = (blobId: string) => {
    const blobInfo = blobs.get(blobId);
    if (blobInfo?.blob) {
      const url = URL.createObjectURL(blobInfo.blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `walrus-blob-${blobId.slice(0, 8)}.bin`;
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  return (
    <div className="blob-list">
      <h2>View Blobs</h2>

      <div className="blob-input">
        <input
          type="text"
          placeholder="Enter Blob ID"
          value={inputBlobId}
          onChange={(e) => setInputBlobId(e.target.value)}
        />
        <button onClick={handleAddBlob}>Load Blob</button>
      </div>

      <div className="blobs">
        {blobIds.map(blobId => {
          const blobInfo = blobs.get(blobId);

          return (
            <div key={blobId} className="blob-item">
              <p><strong>Blob ID:</strong> <code>{blobId}</code></p>

              {!blobInfo && (
                <button onClick={() => loadBlob(blobId)}>Load</button>
              )}

              {blobInfo?.loading && <p>Loading...</p>}

              {blobInfo?.error && (
                <p className="error">Error: {blobInfo.error}</p>
              )}

              {blobInfo?.blob && (
                <div>
                  <p>Size: {(blobInfo.blob.size / 1024).toFixed(2)} KB</p>
                  <p>Type: {blobInfo.blob.type || 'application/octet-stream'}</p>
                  <button onClick={() => downloadBlob(blobId)}>Download</button>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
```

### 7. Configure SPA Routing

Create `ws-resources.json` in your project root:

```json
{
  "routes": {
    "/*": "/index.html"
  },
  "headers": {
    "/": {
      "Content-Type": "text/html",
      "Cache-Control": "public, max-age=3600"
    }
  },
  "ignore": [
    "node_modules",
    "src",
    ".git",
    "*.md"
  ]
}
```

### 8. Add Basic Styles (Optional)

```css
/* src/App.css */
.app {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
}

header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 40px;
  padding-bottom: 20px;
  border-bottom: 2px solid #eee;
}

.file-upload, .blob-list {
  margin-bottom: 40px;
  padding: 20px;
  border: 1px solid #ddd;
  border-radius: 8px;
}

.upload-form {
  display: flex;
  flex-direction: column;
  gap: 15px;
}

.epochs-selector {
  display: flex;
  flex-direction: column;
  gap: 5px;
}

button {
  padding: 10px 20px;
  background: #0066cc;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
}

button:disabled {
  background: #ccc;
  cursor: not-allowed;
}

.error {
  color: red;
  padding: 10px;
  background: #fee;
  border-radius: 4px;
}

.upload-item, .blob-item {
  padding: 15px;
  margin: 10px 0;
  background: #f9f9f9;
  border-radius: 4px;
}

code {
  background: #f0f0f0;
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 0.9em;
  word-break: break-all;
}
```

---

## Deployment

### Build the Application

```bash
# Build for production
npm run build

# Copy ws-resources.json to dist folder
cp ws-resources.json dist/
```

### Deploy to Walrus Sites

```bash
# Deploy to testnet (for testing)
site-builder deploy ./dist --epochs 1

# Deploy to mainnet (for production)
site-builder deploy ./dist --epochs 10
```

After deployment, you'll receive:

```
Created site: https://<site-id>.wal.app
Site Object ID: 0x...
```

### Update Existing Site

```bash
# Make changes to your code
# Rebuild
npm run build
cp ws-resources.json dist/

# Update the existing site (uses same object ID)
site-builder deploy ./dist
```

---

## Important Considerations

### 1. Token Requirements

**WAL Tokens (Storage Fees)**
- Required for storing blobs on Walrus
- Cost depends on blob size and number of epochs
- Approximately 1 WAL per 1 MB per epoch (estimate)

**SUI Tokens (Gas Fees)**
- Required for blockchain transactions
- Each upload creates a transaction on Sui
- Typical cost: 0.001-0.01 SUI per transaction

### 2. Storage Epochs

- **Mainnet**: 1 epoch ≈ 2 weeks
- **Testnet**: 1 epoch ≈ 1 day
- Blobs are stored for the specified number of epochs
- Can be marked as deletable or permanent

### 3. Blob Discovery

Walrus doesn't have built-in blob discovery. To list user's blobs, you need to:

**Option 1: Local Storage** (Current Implementation)
- Store blob IDs in browser localStorage
- Limited to single browser/device

**Option 2: Sui Objects**
- Create custom Sui objects to track blob ownership
- Query Sui blockchain to find user's blobs
- Requires Move smart contract deployment

**Option 3: Backend Service**
- Centralized database to track blob metadata
- Best for production applications

### 4. File Size Limits

- Most public aggregators limit uploads to **10 MiB**
- For larger files, you may need to:
  - Run your own aggregator/publisher
  - Split files into chunks
  - Use upload relay services

### 5. Upload Relay (Optional)

Direct uploads from browser require many requests (~2200 per blob). An upload relay can:
- Reduce client-side complexity
- Improve upload performance
- Handle retry logic

Configure upload relay:

```typescript
const walrusClient = new WalrusClient({
  network: 'mainnet',
  suiClient,
  uploadRelay: 'https://your-relay-url.com'
});
```

### 6. Error Handling

Common errors to handle:

**Insufficient WAL Balance**
```typescript
try {
  await walrusClient.writeFiles({ ... });
} catch (error) {
  if (error.message.includes('insufficient')) {
    alert('Not enough WAL tokens for storage');
  }
}
```

**Epoch Changes**
```typescript
import { RetryableWalrusClientError } from '@mysten/walrus';

try {
  await walrusClient.writeFiles({ ... });
} catch (error) {
  if (error instanceof RetryableWalrusClientError) {
    // Reset client and retry
    walrusClient = createWalrusClient(suiClient);
    await walrusClient.writeFiles({ ... });
  }
}
```

### 7. Security Considerations

- **All blobs are public**: Walrus stores data publicly
- **No built-in encryption**: Encrypt sensitive data client-side before upload
- **Wallet security**: Users must protect their Slush wallet credentials
- **Transaction signing**: Every upload requires user confirmation

### 8. Browser Compatibility

- Modern browsers (Chrome, Firefox, Edge, Safari)
- WebAssembly support required
- LocalStorage enabled

---

## Testing Checklist

Before deployment:

- [ ] Wallet connection works with Slush
- [ ] File upload prompts for transaction signing
- [ ] Blob IDs are displayed after upload
- [ ] Blobs can be retrieved and downloaded
- [ ] Error messages are user-friendly
- [ ] LocalStorage persistence works
- [ ] Responsive design on mobile
- [ ] Test with different file types
- [ ] Test with different file sizes
- [ ] Test epoch expiration behavior

---

## Additional Resources

### Documentation

- [Walrus Documentation](https://docs.wal.app)
- [Walrus Sites Guide](https://docs.wal.app/walrus-sites/intro.html)
- [Sui dApp Kit](https://sdk.mystenlabs.com/dapp-kit)
- [Walrus SDK](https://sdk.mystenlabs.com/walrus)
- [Slush Wallet Integration](https://sdk.mystenlabs.com/dapp-kit/slush)

### Example Projects

- [Walrus Example Sites](https://github.com/MystenLabs/example-walrus-sites)
- [Walrus Docs Examples](https://github.com/MystenLabs/walrus-docs/tree/main/examples)

### Community

- [Sui Developer Forum](https://forums.sui.io)
- [Walrus Discord](https://discord.gg/sui)

---

## Troubleshooting

### WASM Loading Issues

If you see WASM errors:

```typescript
// Explicitly set WASM URL
import walrusWasmUrl from '@mysten/walrus-wasm/web/walrus_wasm_bg.wasm?url';

const walrusClient = new WalrusClient({
  network: 'mainnet',
  suiClient,
  wasmUrl: walrusWasmUrl
});
```

### Wallet Not Connecting

- Clear browser cache
- Reinstall Slush wallet extension
- Check network configuration (mainnet vs testnet)
- Verify wallet has SUI tokens

### Upload Fails

- Check WAL token balance
- Check SUI token balance for gas
- Verify file size < 10 MiB
- Try with smaller epochs value
- Check network connectivity

### Site Not Loading

- Verify site-builder deployment completed
- Check site object ID is correct
- Wait a few minutes for propagation
- Try accessing via different portal
- Check ws-resources.json is in dist folder

---

## Future Enhancements

Potential features to add:

1. **File Preview**: Display images/videos/PDFs directly in browser
2. **Batch Upload**: Upload multiple files at once
3. **Progress Tracking**: Show upload progress percentage
4. **Metadata Storage**: Store file names, descriptions in Sui objects
5. **Sharing**: Generate shareable links for blobs
6. **Encryption**: Client-side encryption for sensitive files
7. **Search**: Full-text search through blob metadata
8. **Gallery View**: Visual grid for image blobs
9. **IPFS Integration**: Store blob IDs on IPFS for backup
10. **Analytics**: Track upload statistics and storage usage

---

## License

This guide is provided as-is for educational purposes. Walrus and Sui are projects by Mysten Labs.

## Support

For issues with this implementation:
1. Check the [Walrus Documentation](https://docs.wal.app)
2. Ask on [Sui Developer Forum](https://forums.sui.io)
3. Join the [Sui Discord](https://discord.gg/sui)

---

**Last Updated**: 2025-10-11
