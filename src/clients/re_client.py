"""
Relation Engine API client
"""
import json
import requests
from urllib.parse import urljoin

from src.utils.get_config import get_config


def save(coll_name, docs):
    """
    Bulk-save documents to the relation engine database
    API docs: https://github.com/kbase/relation_engine_api
    Args:
        coll_name - collection name
        docs - list of dicts to save into the collection as json documents
    """
    config = get_config()
    url = urljoin(config['relation_engine_url'] + '/', 'api/v1/documents')
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    params = {'collection': coll_name, 'on_duplicate': 'update'}
    headers = {'Authorization': config['token']}
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers=headers
    )
    if resp.status_code != 200:
        raise RuntimeError('Error response from relation engine API: %s' % resp.text)
    return resp.json()


def check_for_ws(wsid):
    """
    Check if a workspace has already been imported to arango
    Args:
        wsid - workspace ID
    Returns pair of (result, err)
    """
    config = get_config()
    url = urljoin(config['relation_engine_url'] + '/', 'api/v1/query_results')
    headers = {'Authorization': config['token']}
    query = """
    let ws_ids = @ws_ids
    for o in wsfull_workspace
        filter o._key == @key
        return o._id
    """
    resp = requests.post(
        url,
        data=json.dumps({'query': query, 'key': str(wsid)}),
        headers=headers
    )
    if not resp.ok:
        return (None, resp.text)
    return (resp.json(), None)
