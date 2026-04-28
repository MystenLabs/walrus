// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * Local development server that handles Vercel rewrites
 * This allows testing .md URLs locally before deployment
 * Uses Node.js built-in modules (no dependencies required)
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = 3001;
const BUILD_DIR = path.join(__dirname, '../../build');
const WS_RESOURCES_PATH = path.join(__dirname, '../../ws-resources.json');

// Load route mappings from ws-resources.json
let routeMap = {};
try {
  const wsResources = JSON.parse(fs.readFileSync(WS_RESOURCES_PATH, 'utf8'));
  routeMap = wsResources.routes || {};
} catch { /* fall through without routes */ }

const MIME_TYPES = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.md': 'text/markdown; charset=utf-8',
  '.txt': 'text/plain',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
};

function getContentType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  return MIME_TYPES[ext] || 'application/octet-stream';
}

function serveFile(res, filePath) {
  fs.readFile(filePath, (err, content) => {
    if (err) {
      if (err.code === 'ENOENT') {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
      } else {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('500 Internal Server Error');
      }
    } else {
      const contentType = getContentType(filePath);
      res.writeHead(200, {
        'Content-Type': contentType,
        'Cache-Control': 'public, max-age=3600'
      });
      res.end(content, 'utf-8');
    }
  });
}

const server = http.createServer((req, res) => {
  const parsedUrl = url.parse(req.url);
  let pathname = parsedUrl.pathname;

  // Handle route mappings from ws-resources.json
  if (routeMap[pathname]) {
    const target = routeMap[pathname];
    const filePath = path.join(BUILD_DIR, target);
    if (fs.existsSync(filePath)) {
      serveFile(res, filePath);
      return;
    }
  }

  // Handle .md requests (fallback if not in route map)
  if (pathname.endsWith('.md')) {
    // Try stripping /docs/ prefix and looking in markdown/
    const stripped = pathname.replace(/^\/docs\//, '');
    const candidates = [
      path.join(BUILD_DIR, 'markdown', stripped),
      path.join(BUILD_DIR, 'markdown', stripped.replace(/\.md$/, '/index.md')),
    ];
    for (const filePath of candidates) {
      if (fs.existsSync(filePath)) {
        res.writeHead(200, {
          'Content-Type': 'text/markdown; charset=utf-8',
          'Content-Disposition': 'inline',
          'Cache-Control': 'public, max-age=3600'
        });
        fs.createReadStream(filePath).pipe(res);
        return;
      }
    }
  }

  // Serve static files
  let filePath = path.join(BUILD_DIR, pathname);

  // Check if path exists
  if (!fs.existsSync(filePath)) {
    // Try adding .html extension
    const withHtml = filePath + '.html';
    if (fs.existsSync(withHtml)) {
      filePath = withHtml;
    } else {
      // Try index.html for directory
      filePath = path.join(BUILD_DIR, pathname, 'index.html');
      if (!fs.existsSync(filePath)) {
        // Return 404 instead of SPA fallback (matches production behavior)
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
        return;
      }
    }
  } else if (fs.statSync(filePath).isDirectory()) {
    filePath = path.join(filePath, 'index.html');
  }

  serveFile(res, filePath);
});

server.listen(PORT, () => {
  console.log(`\n🚀 Local test server running!`);
  console.log(`\n📖 Test URLs:`);
  console.log(`   HTML: http://localhost:${PORT}/guides/developer/getting-started`);
  console.log(`   MD:   http://localhost:${PORT}/guides/developer/getting-started.md`);
  console.log(`\n✨ This server mimics Vercel rewrites for local testing`);
  console.log(`\n💡 Press Ctrl+C to stop\n`);
});
