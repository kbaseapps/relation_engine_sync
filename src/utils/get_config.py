import os
from urllib.parse import urljoin

# Prefix used by KBase when setting dynamic service env vars
_ENV_PREFIX = "KBASE_SECURE_CONFIG_PARAM_"


def get_config():
    """Get configuration data from the environment."""
    required_env_vars = [
        'KBASE_ENDPOINT',
        _ENV_PREFIX + 'AUTH_TOKEN'
    ]
    for var in required_env_vars:
        if not os.environ.get(var, '').strip():  # None, blank, or missing
            raise RuntimeError("Missing required env var: " + var)
    kbase_services_url = os.environ['KBASE_ENDPOINT']
    # Get the url of the relation engine API
    relation_engine_url = urljoin(kbase_services_url + '/', 'relation_engine_api')
    # Get the URL of the workspace API
    ws_url = urljoin(kbase_services_url + '/', 'ws')
    # Get the authentication token for making requests to the workspace and relation engine
    token = os.environ[_ENV_PREFIX + 'AUTH_TOKEN']
    return {
        'ws_url': ws_url,
        'relation_engine_url': relation_engine_url,
        'token': token
    }
