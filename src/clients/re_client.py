"""
Relation Engine API client
"""
import os
import json
import requests
from urllib.parse import urljoin


def save(coll_name, docs):
    """Bulk-save documents"""
    (db_url, token) = _api_config()
    url = db_url + '/api/documents'
    print('authorization token len', len(token))
    print('db_url is', db_url)
    print('url is', url)
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    print(payload)
    print(len(docs), 'total docs')
    params = {'collection': coll_name, 'on_duplicate': 'replace'}
    print('params are', params)
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers={'Authorization': token}
    )
    print('=' * 80)
    print('resp', resp)
    print('text', resp.text)
    print('status', resp.status_code)
    print('=' * 80)
    if resp.status_code != 200:
        raise RuntimeError('Error response from relation engine API: %s' % resp.text)
    return resp.json()


def _api_config():
    for required_env in ['KBASE_ENDPOINT', 'KBASE_AUTH_TOKEN']:
        if required_env not in os.environ:
            raise RuntimeError('Missing required environment variable: ' + required_env)
    kbase_services_url = os.environ.get('KBASE_ENDPOINT', 'https://ci.kbase.us/services')
    url = urljoin(kbase_services_url + '/', 'relation_engine_api')
    url = os.environ.get('RELATION_ENGINE_API_URL', url)
    token = os.environ.get('KBASE_AUTH_TOKEN')
    token = os.environ.get('RELATION_ENGINE_API_AUTH_TOKEN', token)
    return (url, token)
