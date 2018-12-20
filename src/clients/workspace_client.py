"""
Make API requests to the kbase workspace JSON RPC server.
"""
import os
import json
import requests


def req(method, params):
    """Make a JSON RPC request to the workspace server."""
    _workspace_url = os.environ.get('KBASE_WORKSPACE_URL', 'https://kbase.us/services/ws/')
    _auth_token = os.environ.get('KBASE_AUTH_TOKEN')
    headers = {'Authorization': _auth_token}
    payload = {'version': '1.1', 'method': method, 'params': [params]}
    resp = requests.post(_workspace_url, data=json.dumps(payload), headers=headers).json()
    if 'error' in resp:
        raise RuntimeError('Error response from workspace:\n%s', str(resp))
    elif 'result' not in resp or not len(resp['result']):
        raise RuntimeError('Invalid workspace response:\n%s' % str(resp))
    else:
        return resp['result'][0]
