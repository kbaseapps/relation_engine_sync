"""
Relation Engine API client
"""
import json
import os
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
    url = urljoin(config['relation_engine_url'] + '/', 'api/documents')
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    params = {'collection': coll_name, 'on_duplicate': 'update'}
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers={'Authorization': config['token']}
    )
    if not resp.ok:
        raise RuntimeError(f'Error response from RE API: {resp.text}')
    return resp.json()


def import_file(file_path, fd):
    """
    Import a file full of json documents, separated by linebreaks.
    """
    config = get_config()
    url = urljoin(config['relation_engine_url'] + '/', 'api/documents')
    # convert the docs into a string, where each obj is separated by a linebreak
    coll_name = os.path.basename(file_path).split('.')[0]
    params = {'collection': coll_name, 'on_duplicate': 'update'}
    resp = requests.put(
        url,
        data=fd,
        params=params,
        headers={'Authorization': config['token']}
    )
    if not resp.ok:
        raise RuntimeError(f'Error response from RE API: {resp.text}')
