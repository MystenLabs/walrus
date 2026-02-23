{/* https://linear.app/mysten-labs/issue/DOCS-673/sitesconfigurationadding-metadata */}

It's possible to make your Site object prettier by displaying information about it on Sui explorers and wallets by using the `metadata` field. This is useful for adding human-readable information about the site. For example, you can add a link to your site's homepage, an image of your site's logo, and so on.

In the previous example, the fields correspond to the basic set of properties suggested by the [Sui Display object standard](https://docs.sui.io/standards/display#display-properties).

```json
"metadata": {
    "link": "https://subdomain.wal.app",
    "image_url": "https://www.walrus.xyz/walrus-site",
    "description": "This is a walrus site.",
    "project_url": "https://github.com/MystenLabs/walrus-sites/",
    "creator": "MystenLabs"
  }
```

All metadata fields are optional and can be omitted if not needed. There are some default values specified in the `site-builder` CLI, which can be overridden by your configuration.

It is recommended to use the above fields like this:

- `link`: Add a link to your site's homepage.

- `image_url`: Include a URL to your site's logo which is displayed on your wallet. For example you can place a link to your sites favicon.

- `description`: Add a brief description of your site.

- `project_url`: If your site is open-source, add a link to your site's GitHub repository.

- `creator`: Add the name of your company, group, or individual creator of your site.