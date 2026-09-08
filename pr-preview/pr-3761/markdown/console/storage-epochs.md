> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Walrus storage is time-bound. Your data stays available for a set amount of storage, measured in epochs, and expires when that storage runs out. Walrus Console tracks and pays for this so you do not manage epochs by hand, and it extends storage automatically before it runs out.

> **Info**
>
> The current beta includes storage, epochs, and automatic extension. For related cost details, see the [concepts and overview](./overview).
## How storage epochs work

Walrus measures storage in epochs, which are fixed periods defined by the Walrus network. One epoch lasts two weeks on Mainnet and one day on Testnet, and storage can be bought for at most 53 epochs at a time. For the current values, see the [Network Reference](/docs/network-reference). When you store a file, you reserve storage for a number of epochs. While that storage lasts, the network keeps your data available. When it runs out and nobody renews it, the data expires and is no longer retrievable.

Console handles the underlying payment and epoch accounting for you through your [Pearl wallet](./auth#your-pearl-wallet). You store a file, and Console reserves and funds its storage. You do not buy epochs or sign renewal transactions yourself.

## Automatic extension

To keep developers from losing data because they forgot to renew, Console extends storage automatically before it expires. A background task checks the files in your account on a schedule, and when a file's storage is close to its end, it extends the storage for more epochs. Data you store through Console stays available without any manual step.

## Checking storage status

Your dashboard shows how much storage each space is using. Storage used and your storage cap also appear per space when you list spaces through the API, and each folder reports an `expires_at` timestamp.

## Storage limits

Each space holds up to 5 GB of storage and five folders, and each upload is capped at 100 MiB.