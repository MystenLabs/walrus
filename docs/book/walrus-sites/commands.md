# Site builder commands

We now describe in more detail the commands available through the site builder.

```admonish tip
In general, the `--help` flag is your friend, you can add it to get further details for the whole
CLI (`site-builder --help`) or individual commands (e.g. `site-builder update --help`).
```

## `deploy`

The `deploy` command is the primary and recommended command for managing your Walrus Site on Sui.
The command takes a directory as input and creates a new Walrus Site from the
resources contained within, and on subsequent calls, it updates the existing site.

### Behavior

The deploy command determines whether to publish a new site or update an existing one by looking for
a Site Object ID. It finds the ID with the following priority:

- An ID provided directly via the `--object-id <OBJECT_ID>` command-line flag.
- An ID found in the object_id field of the ws-resources.json file.
- If no ID is found by either method, deploy will publish a new site.

When a new site is published, its object_id is automatically saved back to `ws-resources.json`,
streamlining future updates.

### Usage

As shown by the command's help information, the typical usage is:

``` sh
site-builder deploy [OPTIONS] --epochs <EPOCHS> <DIRECTORY>
```

The `deploy` command determines whether to publish or update based on the presence of an `object_id`
field in the `ws-resources.json` file. [specifying headers and routing](./routing.md).

```admonish info
The `object_id` field is automatically set by the `deploy`
command, when deploying a new site, so there is no need for manually tracking the Site's Object ID.
```

```admonish note
The wallet you are using to update an existing Site must be the *owner* of the Walrus Site object to be able
to update it.
```

The `--epochs` flag specifies the number of epochs for which the site's resources will be stored
on Walrus (e.g., 10). You can also use max to store for the maximum allowed duration.

```admonish warning
The `--epochs` flag is required & it's value must be greater than 0.
```

```admonish danger title="Epoch duration on Walrus"
On Walrus Testnet, the epoch duration is **one day**.
On Walrus Mainnet, the epoch duration is **fourteen days**.
Therefore, consider storing your site for a large number of epochs
if you want to make it available for the following months!
The maximum duration is set to 53 epochs.
```

If you are just uploading raw files without an `index.html`, you may want to use the
`--list-directory` flag, which will automatically create an index page to browse the files. See for
example <https://bin.wal.app>.

### Migrating from `publish`/`update`

If you have a site that was previously managed with the `publish` and `update` commands, you can
easily switch to the `deploy` command using one of the following methods:

- **Recommended**: Use the `--object-id` cli flag
Simply run the `deploy` command and provide your existing Site's Object ID via the `--object-id` flag:

```sh
site-builder deploy --object-id <YOUR_EXISTING_SITE_ID> --epochs <NUMBER> ./path/to/your/site
```

On success, this will update your site and automatically create (or update if already existing) a
`ws-resources.json` file with the Site's Object ID saved in the `object_id` field.
Future deployments will then be as simple as:

```sh
site-builder deploy --epochs <NUMBER> ./path/to/your/site
```

- Manual `ws-resources.json` setup.
You can manually create or update a `ws-resources.json` file in your site's root directory and add
the `object_id` field with your existing site's Object ID.

```json
{
  "object_id": "0x123...abc"
}
```

Then, you can simply run:

```sh
site-builder deploy --epochs <NUMBER> ./path/to/your/site
```

## `publish`

```admonish note
The `deploy` command is the new standard for publishing and updating your Walrus Sites.
Users are encouraged to migrate to the `deploy` command for a simpler and more robust experience.
```

The `publish` command, as described in the [previous section](./tutorial-publish.md), publishes a
new site on Sui. The command takes a directory as input and creates a new Walrus Site from the
resources contained within.

The `--epochs` flag allows you to specify for how long the site data will be stored on Walrus.

```admonish danger title="Epoch duration on Walrus"
On Walrus Testnet, the epoch duration is **one day**.
On Walrus Mainnet, the epoch duration is **fourteen days**.
Therefore, consider storing your site for a large number of epochs
if you want to make it available for the following months!
The maximum duration is set to 53 epochs.
```

If you are just uploading raw files without an `index.html`, you may want to use the
`--list-directory` flag, which will automatically create an index page to browse the files. See for
example <https://bin.wal.app>.

The `publish` command will also respect the instructions in the `ws-resources.json` configuration
file. To know more, see the section on [specifying headers and routing](./routing.md).

## `update`

```admonish note
The `deploy` command is the new standard for publishing and updating your Walrus Sites.
Users are encouraged to migrate to the `deploy` command for a simpler and more robust experience.
```

This command is the equivalent of `publish`, but for updating an existing site. It takes the same
arguments, with the addition of the Sui object ID of the site to update.

```admonish note
The wallet you are using to call `update` must be the *owner* of the Walrus Site object to be able
to update it.
```

## `convert`

The `convert` command converts an object ID in hex format to the equivalent Base36 format. This
command is useful if you have the Sui object ID of a Walrus Site, and want to know the subdomain
where you can browse it.

## `site-map`

The `sitemap` command shows the resources that compose the Walrus Site at the given object ID.

## `list-directory`

With `list-directory`, you can obtain the `index.html` file that would be generated by running
`publish` or `update` with the `--list-directory` flag. This is useful to see how the index page
would look like before publishing it—and possibly editing to make it look nicer!

## `destroy`

Destroys both blockchain objects and Walrus assets of a site with the given object id.

## `update-resource`

Adds or updates a single resource in a site, eventually replacing any pre-existing ones.
