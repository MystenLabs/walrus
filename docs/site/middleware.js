// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Edge Middleware: content negotiation for markdown, plus server-side AI
// agent tracking via Plausible. Fires a Plausible event when an AI coding
// agent is detected, capturing visits that never execute client-side
// JavaScript.

import { next, rewrite } from '@vercel/functions';

const AI_AGENT_PATTERN =
  /claude[-_]?code|anthropic|cursor|copilot|chatgpt|openai|gptbot|perplexity|cohere|codeium|windsurf|tabnine|sourcegraph|cody/i;

// Results of the .md twin check, per deployment. Files do not change while a
// deployment is live, so a result never goes stale.
const markdownTwinCache = new Map();

function detectServerVisitorType(request) {
  const ua = request.headers.get('user-agent') || '';
  const accept = request.headers.get('accept') || '';
  if (AI_AGENT_PATTERN.test(ua)) return 'agent';
  if (accept.includes('text/markdown')) return 'agent';
  return null;
}

function trackPlausibleEvent(request, visitorType) {
  const url = new URL(request.url);
  const ua = request.headers.get('user-agent') || '';
  const ip = request.headers.get('x-forwarded-for') || request.headers.get('x-real-ip') || '';

  fetch('https://plausible.io/api/event', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'User-Agent': ua,
      'X-Forwarded-For': ip,
    },
    body: JSON.stringify({
      name: 'pageview',
      domain: 'docs.wal.app',
      url: url.toString(),
      referrer: request.headers.get('referer') || '',
      props: { visitor_type: visitorType },
    }),
  }).catch(() => {});
}

function acceptsMarkdown(request) {
  return (request.headers.get('accept') || '').includes('text/markdown');
}

// Returns the page path without a trailing slash, or null when the request is
// not for a page (root, files and assets are left alone).
function toPagePath(pathname) {
  const pagePath = pathname.replace(/\/+$/, '');
  if (!pagePath) return null;
  const lastSegment = pagePath.split('/').pop() || '';
  if (lastSegment.includes('.')) return null;
  return pagePath;
}

async function markdownTwinExists(url) {
  const cacheKey = url.toString();
  if (markdownTwinCache.has(cacheKey)) return markdownTwinCache.get(cacheKey);

  let exists = false;
  try {
    const response = await fetch(url, {
      method: 'HEAD',
      headers: { accept: '*/*' },
      signal: AbortSignal.timeout(2000),
    });
    exists = response.ok || response.status === 405;
  } catch {
    exists = false;
  }

  markdownTwinCache.set(cacheKey, exists);
  return exists;
}

export const config = {
  matcher: [
    '/((?!_next|api|static|img|fonts|favicon).*)',
  ],
};

export default async function middleware(request) {
  const visitorType = detectServerVisitorType(request);
  if (visitorType === 'agent') {
    trackPlausibleEvent(request, visitorType);
  }

  if (acceptsMarkdown(request)) {
    const url = new URL(request.url);
    const pagePath = toPagePath(url.pathname);

    if (pagePath) {
      const markdownUrl = new URL(url);
      markdownUrl.pathname = `${pagePath}.md`;

      if (await markdownTwinExists(markdownUrl)) {
        return rewrite(markdownUrl, { headers: { Vary: 'Accept' } });
      }
    }
  }

  return next({ headers: { Vary: 'Accept' } });
}
