import json
import requests

from .utils.config import get_config

_CONFIG = get_config()


def download_obj(wsid, objid, ver):
    """
    Download an object from the workspace, streaming it to disk.
    """
    ref = '/'.join([str(n) for n in [wsid, objid, ver]])
    resp = requests.post(
        _CONFIG['ws_url'],
        data=json.dumps({
            'method': 'administer',
            'params': [{
                'command': 'getObjects',
                'params': {
                    'objects': [{'ref': ref}],
                    'no_data': 1
                }
            }]
        }),
        headers={'Authorization': _CONFIG['ws_token']}
    )
    resp.raise_for_status()
    return resp.json()['result'][0]['data'][0]
