import os
from urllib.parse import urljoin


def get_config():
    """Get configuration data from the environment."""
    kbase_services_url = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services')
    # Get the url of the relation engine API
    relation_engine_url = urljoin(kbase_services_url + '/', 'relation_engine_api')
    if 'RELATION_ENGINE_API_URL' in os.environ:
        relation_engine_url = os.environ['RELATION_ENGINE_API_URL']
    # Get the URL of the workspace API
    ws_url = urljoin(kbase_services_url + '/', 'ws')
    if 'KBASE_WORKSPACE_URL' in os.environ:
        ws_url = os.environ['KBASE_WORKSPACE_URL']
    # Get the authentication token for making requests to the workspace and relation engine
    token = os.environ['KBASE_AUTH_TOKEN']
    relation_engine_token = token
    if 'RELATION_ENGINE_API_AUTH_TOKEN' in os.environ:
        relation_engine_token = os.environ['RELATION_ENGINE_API_AUTH_TOKEN']
    return {
        'username': os.environ.get('KBASE_USERNAME'),
        'ws_url': ws_url,
        'relation_engine_url': relation_engine_url,
        'token': token,
        'relation_engine_token': relation_engine_token
    }
