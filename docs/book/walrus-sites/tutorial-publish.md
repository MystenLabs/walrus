# Deploying a Walrus Site

Now that everything is installed and configured, you should be able to start publishing your first
Walrus Site!

## Select the source material for the site

The `site-builder` works by uploading a directory of files produced by any web framework to Walrus
and adding the relevant metadata to Sui. This directory should have a file called `index.html` in
its root, which will be the entry point to the Walrus Site.

There is a very useful [example-Walrus-sites](https://github.com/MystenLabs/example-walrus-sites)
repository that contains multiple kinds of sites that you can use for reference.

For simplicity, we will start by publishing the most frugal of the sites, the `walrus-snake` game.

First, clone the repository of the examples:

```sh
git clone https://github.com/MystenLabs/example-walrus-sites.git && cd example-walrus-sites
```

## Deploying the site

Since we have placed the `walrus` and `site-builder` binaries and configuration in their default
locations, deploying the `./walrus-snake` site is as simple as calling again the deploy command:

```sh
site-builder --context=testnet deploy ./walrus-snake --epochs 1
```

```admonish tip
Depending on the network, the duration of an epoch may vary. Currently on Walrus Testnet, the
duration of an epoch is one day. On Mainnet, the duration of an epoch is two weeks.
```

```admonish warning title="Important: Testnet vs Mainnet Access"
**After deploying, how you access your site depends on which network you used:**

- **Mainnet sites**: Can be accessed through any mainnet portal.
<https://wal.app> serves Walrus Sites on mainnet by resolving SuiNS names that point to them.
- **Testnet sites**: Can be accessed through any testnet portal.
Walrus Foundation does not operate a testnet portal. You can [self-host or run one locally](./portal.md).
```

The end of the output should look like the following:

```txt
Execution completed
Resource operations performed:
  - created resource /Oi-Regular.ttf with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBHgAjAg
  - created resource /file.svg with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBAQAMAA
  - created resource /index.html with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBDAAZAA
  - created resource /walrus.svg with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBGQAeAA
The site routes were modified.
Metadata updated.
The site name has been updated.

Created new site!
New site object ID: 0x617221edd060dafb4070b73160ebf535e1516bf7f246890ed35190eba786d7ac
⚠ wal.app only supports sites deployed on mainnet.
     To browse your testnet site, you need to self-host a portal:
     1. For local development: http://2ffmxm7jmglccr79htmpdbaeqezp2krgftue5pfq9f83tdqjsc.localhost:3000
     2. For public sharing: http://2ffmxm7jmglccr79htmpdbaeqezp2krgftue5pfq9f83tdqjsc.yourdomain.com:3000

     📖 Setup instructions: https://docs.wal.app/walrus-sites/portal.html#running-the-portal-locally

     💡 Tip: You may also bring your own domain (https://docs.wal.app/walrus-sites/bring-your-own-domain.html)
            or find third-party hosted testnet portals.
```

```admonish note
Keep in mind that option 2 is only available on `mainnet`.
For testnet sites, use option 1 - see the [portal setup guide](./portal.md) for instructions.
```

This output tells you that, for each file in the folder, a new Walrus quilt was created, and the
respective quilt ID. Further, it prints the object ID of the Walrus Site object on Sui (so you can
have a look in the explorer and use it to set the SuiNS name) and, finally, the URL at which you can
browse the site.
The deploy command will also save this new Site Object ID to the `ws-resources.json`.

Note here that we are implicitly using the default `sites-config.yaml` as the config for the site
builder that we set up previously on the [installation section](./tutorial-install.md). The
configuration file is necessary to ensure that the `site-builder` knows the correct Sui package for
the Walrus Sites logic.

More details on the configuration of the `site-builder` can be found under the [advanced
configuration](./builder-config.md) section.

## Update the site

Let's say now you want to update the content of the site, for example by changing the title from
"eat all the blobs!" to "Glob all the Blobs!".

First, make this edit on in the `./walrus-snake/index.html` file.

Then, you can update the existing site by running the `deploy` command again. The `deploy` command
will use the Site Object ID stored in ws-resources.json (from the initial deployment) to identify
which site to update. You do not need to specify the object ID manually:

```sh
site-builder --context=testnet deploy --epochs 1 ./walrus-snake
```

The output this time should be:

```txt
Execution completed
Resource operations performed:
  - deleted resource /Oi-Regular.ttf with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBHgAjAg
  - deleted resource /file.svg with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBAQAMAA
  - deleted resource /index.html with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBDAAZAA
  - deleted resource /walrus.svg with quilt patch ID Jqz2KSMu18pygjkC-WVEQqtUZRo18-cuf_566VZSxVoBGQAeAA
  - created resource /Oi-Regular.ttf with quilt patch ID BjJAfHLJKMDZ0tFZaLKVw0R74re5RG65-xNhaZ5uwowBHgAjAg
  - created resource /file.svg with quilt patch ID BjJAfHLJKMDZ0tFZaLKVw0R74re5RG65-xNhaZ5uwowBAQAMAA
  - created resource /index.html with quilt patch ID BjJAfHLJKMDZ0tFZaLKVw0R74re5RG65-xNhaZ5uwowBDAAZAA
  - created resource /walrus.svg with quilt patch ID BjJAfHLJKMDZ0tFZaLKVw0R74re5RG65-xNhaZ5uwowBGQAeAA
The site routes were left unchanged.
No Metadata updated.
Site name has not been updated.

Site object ID: 0x4a1be0fb330215c532d74c70d34bc35f185cc7ce025e04b9ad42bc4ac8eda5ce
⚠ wal.app only supports sites deployed on mainnet.
     To browse your testnet site, you need to self-host a portal:
     1. For local development: http://1uhtkoi4t8swxbn2y0mec0l94368nhq2wa1xlh1kc1e43fbzym.localhost:3000
     2. For public sharing: http://1uhtkoi4t8swxbn2y0mec0l94368nhq2wa1xlh1kc1e43fbzym.yourdomain.com:3000

     📖 Setup instructions: https://docs.wal.app/walrus-sites/portal.html#running-the-portal-locally

     💡 Tip: You may also bring your own domain (https://docs.wal.app/walrus-sites/bring-your-own-domain.html)
            or find third-party hosted testnet portals.
```

Browsing to the provided URL should reflect the change. You've updated the site!

Notice that all site resources are deleted (because they all belong to the same quilt), even those
that haven't been modified. They then get re-uploaded together as a single quilt, where each
resource corresponds to a quilt patch. This happens because the site-builder stores files on Walrus
using quilts by default. This approach offers significant benefits: faster upload speeds and lower
storage costs, especially when uploading many small files. The only disadvantage is that you cannot
update a single file within a quilt—if even a tiny file changes, the entire quilt must be
re-uploaded. If you want to tweak with the updates mechanism, you should check out the `--strategy`
flag, but this is something to cover in a following section.

```admonish note
The wallet you are using must be the *owner* of the Walrus Site object to be able to update it.
```
