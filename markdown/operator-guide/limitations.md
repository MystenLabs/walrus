By default, [store blob](/docs/http-api/storing-blobs#store) requests are limited to 10 MiB. You can increase this limit through the `--max-body-size` option. [Store quilt](/docs/http-api/storing-blobs#storing-quilts) requests are limited to 100 MiB by default, and you can increase them using the `--max-quilt-body-size` option.

If you host the aggregator or publisher on a third-party platform, ensure that any additional platform-imposed limits or constraints are compatible with your intended use case.

## Configure nginx caching {#nginx-caching}

If you run a public aggregator, deploy a caching reverse proxy in front of it to reduce load on Walrus storage nodes. The aggregator acts as an origin server behind the cache.

### Prepare a cache directory

Mount the cache on high-performance drives. There are 2 options for improved I/O:

**Option 1: RAID 0:**

This example uses 2 disks.

```sh
sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=2 /dev/nvme0n1 /dev/nvme1n1
sudo mkfs.ext4 /dev/md0
sudo mkdir -p /cache
sudo mount /dev/md0 /cache
echo '/dev/md0 /cache ext4 defaults,nofail 0 0' | sudo tee -a /etc/fstab
```

**Option 2: Ramdisk:**

This example assumes you have sufficient free RAM.

```sh
sudo mkdir -p /cache
sudo mount -t tmpfs -o size=16G aggregator-cache /cache
echo 'tmpfs /cache tmpfs nodev,nosuid,nodiratime,size=16G 0 0' | sudo tee -a /etc/fstab
```

### Install nginx and certbot

```sh
sudo apt install nginx python3-certbot-nginx -y
```

If you have not installed certbot from the [Storage Node Setup](/docs/operator-guide/storage-nodes/storage-node-setup#tls-setup) guide, install it now:

```sh
sudo snap install --classic certbot --channel=edge
```

### Configure nginx caching

To configure the nginx cache, follow these steps:

##### Step 1: Set permissions on the cache directory.

```sh
sudo chown www-data:www-data /cache
sudo chmod 755 /cache
```

##### Step 2: Add a cache path directive.

Add the following to the `http` block in `/etc/nginx/nginx.conf`:

```nginx
http {
    # ...
    proxy_cache_path /cache levels=1:2 keys_zone=agg_cache:10m max_size=16g
                     inactive=1h use_temp_path=off;
    # ...
}
```

##### Step 3: Create a site configuration.

Create a file at `/etc/nginx/sites-available/YOUR_HOSTNAME`:

```nginx
server {
    listen 443 ssl;
    server_name <YOUR_HOSTNAME>;

    # SSL configuration (paths will be updated by certbot)
    ssl_certificate /etc/letsencrypt/live/<YOUR_HOSTNAME>/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/<YOUR_HOSTNAME>/privkey.pem;

    # Proxy traffic to the aggregator backend at localhost:9000
    location / {
        proxy_pass http://localhost:9000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Enable caching
        proxy_cache agg_cache;
        proxy_cache_bypass $http_cache_control;
        # Respect upstream Cache-Control and Expires headers;
        # the values below are fallback values
        proxy_cache_valid 200 302 10m;
        proxy_cache_valid 404 1m;
        proxy_cache_use_stale error timeout invalid_header updating
                              http_500 http_502 http_503 http_504;

        add_header X-Cache-Status $upstream_cache_status;
    }
}
```

##### Step 4: Validate the configuration.

Run `sudo nginx -t`.

##### Step 5: Obtain a TLS certificate.

```sh
sudo certbot --nginx -d YOUR_HOSTNAME
```

##### Step 6: Enable the site and start nginx.

```sh
sudo ln -s /etc/nginx/sites-available/YOUR_HOSTNAME /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo systemctl restart nginx
```

:::caution

If you run the nginx reverse proxy on the **same host** as the storage node, change `standalone` to `nginx` in `/etc/letsencrypt/renewal/walrus-storage-node.conf`. This ensures certbot uses the nginx plugin for renewal instead of trying to bind to **port 80** directly. See the [Storage Node Setup TLS section](/docs/operator-guide/storage-nodes/storage-node-setup#tls-setup) to learn more about how certbot is configured for the storage node.

:::

## View daemon metrics

Services export a metrics endpoint by default, accessible at `http://127.0.0.1:27182/metrics`. You can change the address using the `--metrics-address METRICS_ADDRESS` CLI option.