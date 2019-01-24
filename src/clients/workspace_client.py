"""
Make API requests to the kbase workspace JSON RPC server.
"""
import json
import requests
from utils.get_config import get_config


def req(method, params):
    """
    Make a JSON RPC request to the workspace server.
    KIDL docs: https://kbase.us/services/ws/docs/Workspace.html
    """
    config = get_config()
    payload = {'version': '1.1', 'method': method, 'params': [params]}
    return _post_req(payload, config)


def admin_req(method, params):
    """
    Make a JSON RPC administration request (method is wrapped inside the "administer" method).
    Docs for this interface: https://kbase.us/services/ws/docs/administrationinterface.html
    """
    config = get_config()
    payload = {
        'version': '1.1',
        'method': 'Workspace.administer',
        'params': [{'command': method, 'params': params}]  # , 'user': config['username']}]
    }
    return _post_req(payload, config)


def _post_req(payload, config):
    """Make a post request to the workspace server and process the response."""
    headers = {'Authorization': config['token']}
    resp = requests.post(config['ws_url'], data=json.dumps(payload), headers=headers)
    if not resp.ok:
        raise RuntimeError('Error response from workspace:\n%s' % resp.text)
    resp_json = resp.json()
    if 'error' in resp_json:
        raise RuntimeError('Error response from workspace:\n%s' % resp.text)
    elif 'result' not in resp_json or not len(resp_json['result']):
        raise RuntimeError('Invalid workspace response:\n%s' % resp.text)
    return resp_json['result'][0]
