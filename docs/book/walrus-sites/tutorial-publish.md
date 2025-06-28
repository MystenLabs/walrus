# Publishing a Walrus Site

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

``` sh
git clone https://github.com/MystenLabs/example-walrus-sites.git && cd example-walrus-sites
```

## Publish the site

Since we have placed the `walrus` and `site-builder` binaries and configuration in their default
locations, publishing the `./walrus-snake` site is as simple as calling the publishing command:

``` sh
site-builder publish ./walrus-snake --epochs 1
```

``` admonish tip
Depending on the network, the duration of an epoch may vary. Currently on Walrus Testnet, the
duration of an epoch is one day. On Mainnet, the duration of an epoch is two weeks.
```

The end of the output should look like the following:

``` txt
Execution completed
Resource operations performed:
  - created resource /.DS_Store with blob ID PwNzE9_a9anYb8AZysafQZGqd4h0scsTGhzF2GPsWmQ
  - created resource /Oi-Regular.ttf with blob ID KUTTV_95_c68oQhaRP97tDPOYu0vqCWiGL7mzOq1faU
  - created resource /file.svg with blob ID oUpm044qBN1rkyIJYvMB4dUj6bRe3QEvJAN-cvlIFmk
  - created resource /index.html with blob ID AR03hvxSlyfYl-7MhXct4y3rnIIGPHdnjiIF03BK_XY
  - created resource /walrus.svg with blob ID xK8K1Q5khrl3eBT4jEiB-L_gyShEIOVWti8DcAoEjtw
The site routes were modified

Created new site: test site
New site object ID: 0xe674c144119a37a0ed9cef26a962c3fdfbdbfd86a3b3db562ee81d5542a4eccf
To browse the site, you have the following options:
        1. Run a local portal, and browse the site through it: e.g. http://5qs1ypn4wn90d6mv7d7dkwvvl49hdrlpqulr11ngpykoifycwf.localhost:3000
           (more info: https://docs.wal.app/walrus-sites/portal.html#running-the-portal-locally)
        2. Use a third-party portal (e.g. wal.app), which will require a SuiNS name.
           First, buy a SuiNS name at suins.io (e.g. example-domain), then point it to the site object ID.
           Finally, browse it with: https://example-domain.wal.app
```

``` admonish note
Keep in mind that option 2 is only available on `mainnet`.
```

This output tells you that, for each file in the folder, a new Walrus blob was created, and the
respective blob ID. Further, it prints the object ID of the Walrus Site object on Sui (so you can
have a look in the explorer and use it to set the SuiNS name) and, finally, the URL at which you can
browse the site.
The deploy command will also save this new Site Object ID to the ws-resources.json

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

Then, you can update the existing site by running the `deploy` command again. The deploy command will
use the Site Object ID stored in ws-resources.json (from the initial deployment) to identify which site
to update. You do not need to specify the object ID manually:

``` sh
site-builder deploy --epochs 1 ./walrus-snake
```

The output this time should be:

``` txt
Execution completed
Resource operations performed:
  - deleted resource /index.html with blob ID LVLk9VSnBrEgQ2HJHAgU3p8IarKypQpfn38aSeUZzzE
  - created resource /index.html with blob ID pcZaosgEFtmP2d2IV3QdVhnUjajvQzY2ev8d9U_D5VY
The site routes were left unchanged

Site object ID: 0xe674c144119a37a0ed9cef26a962c3fdfbdbfd86a3b3db562ee81d5542a4eccf
To browse the site, you have the following options:
        1. Run a local portal, and browse the site through it: e.g. http://2ql9wtro4xf2x13pm9jjeyhhfj28okawz5hy453hkyfeholy6f.localhost:3000
           (more info: https://docs.wal.app/walrus-sites/portal.html#running-the-portal-locally)
        2. Use a third-party portal (e.g. wal.app), which will require a SuiNS name.
           First, buy a SuiNS name at suins.io (e.g. example-domain), then point it to the site object ID.
           Finally, browse it with: https://example-domain.wal.app
```

Compared to the when the site was first published, we can see that now the only actions performed
were to delete the old `index.html`, and update it with the newer one.

Browsing to the provided URL should reflect the change. You've updated the site!

```admonish note
The wallet you are using must be the *owner* of the Walrus Site object to be able to update it.
```

```admonish danger title="Extending the expiration date of an existing site"
To extend the expiration date of a previously-stored site, use the `update` command with the
`--check-extend` flag. With this flag, the site-builder will force a check of the status of
all the Walrus blobs composing the site, and will extend the ones that expire before `--epochs`
epochs. This is useful to ensure all the resources in the site are available for the same amount
of epochs.
```
