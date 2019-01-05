"""
Make API requests to the kbase workspace JSON RPC server.
"""
import os
import json
import requests


def req(method, params):
    """Make a JSON RPC request to the workspace server."""
    config = _get_config()
    payload = {'version': '1.1', 'method': method, 'params': [params]}
    return _post_req(payload, config)


def admin_req(method, params):
    """Make a JSON RPC administration request (method is wrapped inside the "administer" method)."""
    config = _get_config()
    payload = {
        'version': '1.1',
        'method': 'administer',
        'params': [{'command': method, 'params': params, 'user': config['user']}]
    }
    return _post_req(payload, config)


def _post_req(payload, config):
    """Make a post request to the workspace server and process the response."""
    resp = requests.post(config['ws_url'], data=json.dumps(payload), headers=config['headers'])
    if not resp.ok:
        raise RuntimeError('Error response from workspace:\n%s' % resp.text)
    resp_json = resp.json()
    if 'error' in resp_json:
        raise RuntimeError('Error response from workspace:\n%s' % resp.text)
    elif 'result' not in resp_json or not len(resp_json['result']):
        raise RuntimeError('Invalid workspace response:\n%s' % resp.text)
    return resp_json['result'][0]


def _get_config():
    token = os.environ.get('KBASE_AUTH_TOKEN')
    return {
        'user': os.environ.get('KBASE_USERNAME'),
        'ws_url': os.environ.get('KBASE_WORKSPACE_URL', 'https://kbase.us/services/ws/'),
        'token': token,
        'headers': {'Authorization': token}
    }
