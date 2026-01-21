Walrus developer documentation, hosted using Docusaurus deployed on a Walrus Site: https://docs.wal.app/

## Content
- `/docs/content/blog/`: Walrus blog posts.
- `/docs/content/design/`: Walrus design documentation.
- `/docs/content/dev-guide/`: Walrus developer guides.
- `/docs/content/legal/`: Walrus terms of service.
- `/docs/content/operator-guide/`: Walrus operator guides.
- `/docs/content/usage/`: Usage documentation.
- `/docs/content/walrus-sites/`: Walrus Sites documentation.

## Style guide

The Walrus documentation uses the Sui Style Guide:
https://docs.sui.io/style-guide

## Custom components

This Docusaurus deployment uses custom TSX/JSX components that expand upon
the basic Docusaurus features. These same components are also used by the Sui,
SuiNS, and (soon) Seal documentation.

To maintain these components, they are housed in a shared repo and pulled into this repo as a subtree using the command:

```
git subtree pull --prefix=docs/site/src/shared https://github.com/MystenLabs/ML-Shared-Docusaurus.git master --squash
```

[Learn more about Git Subtrees](https://www.atlassian.com/git/tutorials/git-subtree).

## Ongoing and planned updates

- TODO(DOCS-345): Revisions to the information architecture.
- TODO(DOCS-594): Revisions to design/operations-sui
- TODO(DOCS-593): Revisions to walrus-sites/tutorial-install
- TODO(DOCS-592): Revisions to walrus-sites/restrictions
- TODO(DOCS-591): Revisions to /docs/walrus-sites/redirects
- TODO(DOCS-590): Revisions to /docs/walrus-sites/bring-your-own-domain
- TODO(DOCS-589): Fix outdated SuiNS tutorial
- TODO(DOCS-585): Information about running a storage node
- TODO(DOCS-584): Revisions to /docs/operator-guide/aggregator
- TODO(DOCS-583): Revisions to /dev-guides/sui-struct
- TODO(DOCS-582): Revisions to  /dev-guide/components
- TODO(DOCS-617): Add blob sizes and memory requirements
- TODO(DOCS-610): Plan expanded code examples
- TODO(DOCS-611): Create new code examples
- TODO(DOCS-609): Add HTTP API to introduction/onboarding
- TODO(DOCS-608): Revise pages for Object Storage mention
