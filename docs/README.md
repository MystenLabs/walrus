Walrus developer documentation, hosted using Docusaurus deployed on a Walrus Site: https://docs.wal.app/index.html

## Content 

- `/docs/content/blog/`: Walrus blog posts.
- `/docs/content/design/`: Walrus design documentation.
- `/docs/content/dev-guide/`: Walrus developer guides.
- `/docs/content/legal/`: Walrus terms of service.
- `/docs/content/operator-guide/`: Walrus operator guides.
- `/docs/content/usage/`: Usage documentation.
- `/docs/content/walrus-sites/`: Walrus Sites documentation.

## Style guide

The Walrus documentation uses the Sui Style Guide: https://docs.sui.io/style-guide

## Custom components 

This Docusaurus deployment uses custom TSX/JSX components that expand upon the basic Docusaurus features. These same components are also used by the Sui, SuiNS, and (soon) Seal documentation.

To maintian these components, they will be consolidated into a central package and added as an NPM dependency. Work for this task is tracked via DOCS-365. 

## Ongoing and planned updates

- DOCS-342 + DOCS-384 + DOCS-524: Revisions for style guide compliance, readablity, and other misc backlog edits.
- DOCS-345: Revisions to the information architecture.
- DOCS-365: Create central package for custom components. 
