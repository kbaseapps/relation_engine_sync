# Sync from the workspace to the relation engine

Pull data from the KBase workspace and upload it into the relation engine graph database.

## Development

Environment variables (you can place these in `.env` in the root of the repo):
* `RELATION_ENGINE_API_URL`
* `RELATION_ENGINE_API_AUTH_TOKEN`
* `KBASE_WORKSPACE_URL`
* `KBASE_AUTH_TOKEN`

To run:

* Install [kbase-sdk 2](https://github.com/jayrbolton/kbase_sdk_cli)
* Run `kbase-sdk test`
