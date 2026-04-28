// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// WebMCP: expose documentation tools to AI agents via the browser.
// https://webmachinelearning.github.io/webmcp/

if (typeof window !== 'undefined') {
  const registerTools = () => {
    if (!navigator.modelContext?.provideContext) return;

    navigator.modelContext.provideContext({
      tools: [
        {
          name: 'search_walrus_docs',
          description:
            'Search the Walrus documentation site for pages matching a query. Returns a search URL.',
          inputSchema: {
            type: 'object',
            properties: {
              query: {
                type: 'string',
                description: 'The search query to find relevant documentation pages.',
              },
            },
            required: ['query'],
          },
          async execute({ query }) {
            const searchUrl = `${window.location.origin}/search?q=${encodeURIComponent(query)}`;
            return {
              type: 'text',
              text: `Search Walrus docs for "${query}": ${searchUrl}`,
            };
          },
        },
        {
          name: 'get_page_content',
          description:
            'Get the full markdown content of the current documentation page.',
          inputSchema: {
            type: 'object',
            properties: {},
          },
          async execute() {
            const path = window.location.pathname;
            const mdPath = path.endsWith('/') ? path.slice(0, -1) : path;
            try {
              const resp = await fetch(`${window.location.origin}/markdown${mdPath}.md`);
              if (resp.ok) {
                const text = await resp.text();
                return { type: 'text', text };
              }
            } catch (e) {
              // Fall through
            }
            const article = document.querySelector('article') || document.querySelector('main');
            return {
              type: 'text',
              text: article ? article.innerText : document.body.innerText.slice(0, 10000),
            };
          },
        },
        {
          name: 'get_page_metadata',
          description:
            'Get metadata about the current page including title, description, URL, and table of contents.',
          inputSchema: {
            type: 'object',
            properties: {},
          },
          async execute() {
            const title = document.title;
            const description =
              document.querySelector('meta[name="description"]')?.content || '';
            const url = window.location.href;
            const tocLinks = document.querySelectorAll('.table-of-contents__link');
            const toc = Array.from(tocLinks).map((link) => ({
              text: link.textContent.trim(),
              id: link.getAttribute('href')?.replace('#', '') || '',
            }));
            const breadcrumbs = Array.from(
              document.querySelectorAll('.breadcrumbs__link'),
            ).map((el) => el.textContent.trim());

            return {
              type: 'text',
              text: JSON.stringify({ title, description, url, breadcrumbs, toc }, null, 2),
            };
          },
        },
        {
          name: 'list_sidebar_pages',
          description:
            'List all pages in the current documentation section from the sidebar navigation.',
          inputSchema: {
            type: 'object',
            properties: {},
          },
          async execute() {
            const links = document.querySelectorAll('.menu__link');
            const pages = Array.from(links).map((link) => ({
              title: link.textContent.trim(),
              href: link.getAttribute('href') || '',
              active: link.classList.contains('menu__link--active'),
            }));
            return { type: 'text', text: JSON.stringify(pages, null, 2) };
          },
        },
        {
          name: 'get_walrus_api_reference',
          description:
            'Get a summary of available Walrus APIs including the HTTP aggregator and publisher endpoints.',
          inputSchema: {
            type: 'object',
            properties: {},
          },
          async execute() {
            try {
              const resp = await fetch('/.well-known/api-catalog');
              if (resp.ok) {
                const catalog = await resp.json();
                return { type: 'text', text: JSON.stringify(catalog, null, 2) };
              }
            } catch (e) {
              // Fall through
            }
            return {
              type: 'text',
              text: JSON.stringify({
                apis: [
                  {
                    name: 'Walrus HTTP API',
                    docs: 'https://docs.wal.app/docs/http-api',
                  },
                  {
                    name: 'Walrus CLI',
                    docs: 'https://docs.wal.app/docs/client-cli',
                  },
                ],
              }),
            };
          },
        },
      ],
    });
  };

  if (document.readyState === 'complete') {
    registerTools();
  } else {
    window.addEventListener('load', registerTools);
  }
}
