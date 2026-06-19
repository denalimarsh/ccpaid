# ccpaid

`ccpaid` runs Claude Code and submits its Anthropic token usage to Paid for a customer selected at session start.

## Usage

```sh
bun run ccpaid.ts -- [claude args...]
```

Build a standalone binary:

```sh
bun build ./ccpaid.ts --compile --outfile ccpaid
./ccpaid -- [claude args...]
```

## Configuration

`PAID_API_KEY` is required. Optional settings:

- `--external-product-id <id>` or `PAID_EXTERNAL_PRODUCT_ID`
- `--flush-ms <ms>` or `CCPAID_FLUSH_MS`
- `--debug` or `CCPAID_DEBUG=1` to write compact telemetry diagnostics
- `--log-file <path>` or `CCPAID_LOG_FILE` to enable diagnostics at a specific path
- `CCPAID_QUERY_SOURCE_ALLOW` / `CCPAID_QUERY_SOURCE_DENY` as comma-separated `query_source` filters

No log file is created unless `--debug`, `CCPAID_DEBUG`, `--log-file`, or `CCPAID_LOG_FILE` is set.

Everything after `--` is passed to `claude`.
