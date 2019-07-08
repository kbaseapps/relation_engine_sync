"""
Relation Engine API client
"""
import json
import os
import requests
from urllib.parse import urljoin

from .config import get_config

_CONFIG = get_config()


def get_doc(coll, key):
    """Fetch a doc in a collection by key."""
    resp = requests.post(
        _CONFIG['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': "let ws_ids = @ws_ids for v in @@coll filter v._key == @key limit 1 return v",
            '@coll': coll,
            'key': key
        }),
        headers={'Authorization': _CONFIG['re_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def get_edge(coll, from_key, to_key):
    """Fetch an edge by from and to keys."""
    query = """
    let ws_ids = @ws_ids
    for v in @@coll
        filter v._from == @from AND v._to == @to
        limit 1
        return v
    """
    resp = requests.post(
        _CONFIG['re_api_url'] + '/api/v1/query_results',
        data=json.dumps({
            'query': query,
            '@coll': coll,
            'from': from_key,
            'to': to_key
        }),
        headers={'Authorization': _CONFIG['re_token']}
    )
    if not resp.ok:
        raise RuntimeError(resp.text)
    return resp.json()


def save(coll_name, docs):
    """
    Bulk-save documents to the relation engine database
    API docs: https://github.com/kbase/relation_engine_api
    Args:
        coll_name - collection name
        docs - list of dicts to save into the collection as json documents
    """
    url = _CONFIG['re_api_url'] + '/api/documents'
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    params = {'collection': coll_name, 'on_duplicate': 'update'}
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers={'Authorization': _CONFIG['ws_token']}
    )
    if not resp.ok:
        raise RuntimeError(f'Error response from RE API: {resp.text}')
    return resp.json()


def import_file(file_path, fd):
    """
    Import a file full of json documents, separated by linebreaks.
    """
    url = urljoin(_CONFIG['re_api_url'] + '/', 'api/documents')
    # convert the docs into a string, where each obj is separated by a linebreak
    coll_name = os.path.basename(file_path).split('.')[0]
    params = {'collection': coll_name, 'on_duplicate': 'update'}
    resp = requests.put(
        url,
        data=fd,
        params=params,
        headers={'Authorization': _CONFIG['token']}
    )
    if not resp.ok:
        raise RuntimeError(f'Error response from RE API: {resp.text}')
