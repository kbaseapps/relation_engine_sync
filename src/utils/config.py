import os
import functools

# Prefix used by KBase when setting dynamic service env vars
_ENV_PREFIX = "KBASE_SECURE_CONFIG_PARAM_"


def _get_env(name, default):
    """Get an env var with the kbase sdk prefix."""
    return os.environ.get(_ENV_PREFIX + name, default)


@functools.lru_cache(maxsize=2)
def get_config():
    """Get configuration data from the environment."""
    # Get the authentication token for making requests to the workspace and relation engine
    ws_token = os.environ[_ENV_PREFIX + 'WS_TOKEN']
    re_token = os.environ[_ENV_PREFIX + 'RE_TOKEN']
    # Get the url of the relation engine API
    re_url = _get_env('RE_URL', 'http://re_api:5000').strip('/')
    # Get the URL of the workspace API
    ws_url = _get_env('WORKSPACE_URL', 'http://workspace:5000').strip('/')
    return {
        'ws_url': ws_url,
        're_api_url': re_url,
        'ws_token': ws_token,
        're_token': re_token,
        'num_consumers': _get_env('NUM_CONSUMERS', 8),
        'kafka_server': _get_env('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': _get_env('KAFKA_CLIENTGROUP', 'releng_sync'),
        'kafka_topics': {
            'workspace_events': _get_env('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            're_admin_events': _get_env('RE_WS_ADMIN_TOPIC', 're_admin_events'),
        }
    }
