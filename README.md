# KBase - Kafka to Relation Engine sync service

This is a small service daemon that syncs from workspace updates (via Kafka) to the Relation Engine (the Arango database).

## Development

Environment variables for deployment

- `KBASE_SECURE_CONFIG_PARAM_WS_TOKEN`
- `KBASE_SECURE_CONFIG_PARAM_RE_TOKEN`
- `KBASE_SECURE_CONFIG_PARAM_WORKSPACE_URL`
- `KBASE_SECURE_CONFIG_PARAM_RE_URL`

Run tests:

```sh
make test
```
