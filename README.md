# Sync from the workspace to the relation engine

Pull data from the KBase workspace and upload it into the relation engine graph database.

## Development

Environment variables (you can place these in `.env` in the root of the repo):
* `KBASE_ENDPOINT` (only required if the below 2 are not set)
* `RELATION_ENGINE_API_URL` (defaults to `<KBASE_ENDPOINT>/relation_engine_api`)
* `KBASE_WORKSPACE_URL` (defaults to `<KBASE_ENDPOINT>/ws`)
* `KBASE_AUTH_TOKEN` (required)
* `RELATION_ENGINE_API_AUTH_TOKEN` (defaults to `KBASE_AUTH_TOKEN`)

To run:

* Install [kbase-sdk 2](https://github.com/jayrbolton/kbase_sdk_cli)
* Run `kbase-sdk test`
