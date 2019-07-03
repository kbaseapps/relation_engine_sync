import os

# Prefix used by KBase when setting dynamic service env vars
_ENV_PREFIX = "KBASE_SECURE_CONFIG_PARAM_"


def get_config():
    """Get configuration data from the environment."""
    required_env_vars = [
        _ENV_PREFIX + 'WS_TOKEN'
    ]
    for var in required_env_vars:
        if not os.environ.get(var, '').strip():  # None, blank, or missing
            raise RuntimeError("Missing required env var: " + var)
    # Get the url of the relation engine API
    re_url = os.environ.get(_ENV_PREFIX + 'RE_URL', 'http://re_api:5000').strip('/')
    # Get the URL of the workspace API
    ws_url = os.environ.get(_ENV_PREFIX + 'WORKSPACE_URL', 'http://workspace:5000').strip('/')
    # Get the authentication token for making requests to the workspace and relation engine
    ws_token = os.environ[_ENV_PREFIX + 'WS_TOKEN']
    return {
        'ws_url': ws_url,
        're_api_url': re_url,
        'ws_token': ws_token,
        'num_consumers': os.environ.get('NUM_CONSUMERS', 8),
        'kafka_server': os.environ.get('KAFKA_SERVER', 'kafka'),
        'kafka_clientgroup': os.environ.get('KAFKA_CLIENTGROUP', 're_sync'),
        'kafka_topics': {
            'workspace_events': os.environ.get('KAFKA_WORKSPACE_TOPIC', 'workspaceevents'),
            're_admin_events': os.environ.get('RE_WS_ADMIN_TOPIC', 're_admin_events'),
        }
    }
