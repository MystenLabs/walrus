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
