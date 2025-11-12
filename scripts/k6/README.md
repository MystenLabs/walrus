# Walrus Performance Tests using K6

This repository contains load tests using the [Grafana k6][k6] load generation
utility, which is capable of generating large amount of loads to HTTP endpoints.

## Features
Additionally, test scripts inherit the following features:

- The ability to ramp up and down arrival rate or number of concurrent requests
  over time.
- End of test summaries with an overview of the performance for different metrics.
- Live web dashboard for test progress, along with end of test HTML reports.
- Test performance metrics exported directly to prometheus.
- Ease of adding new tests, most of the code is just configuring the load
  profile and allowing customisation.

## Running a test script

> Note that the instructions here detail how to manually run k6 scripts that you may be
> developing or expeirmenting with. See the `walrus-performance-tests` crate for running these
> tests with varying parameters against a deployment.

First, ensure that k6 is installed. On MacOs, this can be done with `brew`:
```shell
brew install k6
```

Scripts can then be run with `k6 run <path to script>`.

### An example

For the following example we need a large binary file as an input source.
Generate a 500 MiB file in the script's default search location with the command
```shell
head -c 524288000 </dev/urandom >scripts/k6/data.bin
```

If you would like to view the web dashboard and get an HTML report, set the
following environment variables:
```shell
export K6_WEB_DASHBOARD=true
export K6_WEB_DASHBOARD_EXPORT=report.html
```

The following command will store 1KiB files on **testnet** using the
testnet publisher and report the metrics:
```shell
k6 run scripts/k6/src/tests/publisher/publisher_v1_put_blob_latency.ts
```
The web dashboard, if enabled, is viewable on http://127.0.0.1:5665.

### Useful arguments and environment variables

The following is useful to know.

**Read the test script files**. Each test script file is its own "executable"
and can have different parameters and load profiles. Read the scripts to see
what they do and how they can be called.

**Passing script arguments**. Scripts can be written to accept arguments.
These are passed with repeated `--env` flags.

For example, the script `publisher_v1_put_blob_latency.ts` has been written to accept
the arguments `WALRUS_K6_PAYLOAD_SIZE` and `WALRUS_K6_BLOB_COUNT`, among others. The above example
could therefore be run as
```shell
k6 run --env WALRUS_K6_PAYLOAD_SIZE=1Mi --env WALRUS_K6_BLOB_COUNT=10 \
    scripts/k6/src/tests/publisher/publisher_v1_put_blob_latency.ts
```
to sequentially store ten, 1 MiB files with the publisher.

**Web dashboard and HTML report**. By setting the `K6_WEB_DASHBOARD` and
`K6_WEB_DASHBOARD_EXPORT` you can view the web dashboard and get an HTML report
once the test is done:
```shell
K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_EXPORT=report.html k6 run ...
```

**Prometheus export**. Metrics to prometheus can be exported by setting the
associated [environment variables][k6-prometheus]. For an example of this, see
the sui-operations repository.

## Structure

The k6 scripts directory has the following structure:

```
scripts/k6/src
├── config
│   └── environment.ts
├── flows
│   └── publisher.ts
├── lib
│   └── utils.ts
└── tests
    └── publisher
    └── aggregator
```

- **`tests/`** contains the test scripts that can be run and which utilise
  the helpers in `config/`, `flows/`, and `lib/`. They are written in
  Javascript or Typescript but do not require additional tooling to write or
  transpile.
- **`flows/`** abstracts the HTTP requests to the publisher and aggregator.
- **`config/environment.ts`** contains some default arguments, such as the
  publisher's URL, for walrus-testnet and localhost.
- **`lib/utils.ts`** contains various utilities and sundry code.

## Limitations

The major limitation for these scripts is the persistence of state across tests.
Since the k6 is built with the aim distributed load testing (i.e., running from
different hosts), access to the filesystem is limited.

The current approaches around this topic is to use a redis instance to persist stored blob IDs and
quilt patch IDs for subsequent tests. A local redis instance can be run with:
```
docker  run --rm --name redis -p 6379:6379 redis
```

[k6]: https://k6.io/
[k6-prometheus]: https://grafana.com/docs/k6/latest/results-output/real-time/prometheus-remote-write/
