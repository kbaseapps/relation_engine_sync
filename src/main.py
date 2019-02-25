"""
This file contains all the methods for this KBase SDK module.

Update graph data in the Relation Engine based on updates on KBase.

The object provenance graph has these vertices:
    - wsprov_object -- workspace object
    - wsprov_action -- any action, such as import, copy, or module run
and these edges:
    - wsprov_input_in -- ws object was input in action
    - wsprov_produced - action produced a ws object
    - wsprov_similar_to -- ws object similar to another object somehow (eg
        genome homology)
"""
import traceback

from clients import workspace_client
from clients import re_client
from utils.get_config import get_config
from generate_workspace_infos import generate_workspace_infos
from generate_workspace_perms import generate_workspace_perms
from generate_objects_and_edges import generate_objects_and_edges
from utils.split import split

# In arango, the upa "1/2/3" is stored as "1:2:3"
_UPA_DELIMITER = ':'


def import_workspaces(params):
    """
    Import workspace infos.
    Uses the 'wsfull' namespace.
    Pass in the start and stop for the workspace id range (eg. 1 to 100000)
    """
    vert_name = 'wsfull_workspace'
    errs = []
    # Import workspaces in batches of 1000
    for responses in split(generate_workspace_infos(params['start'], params['stop']), 1000):
        ws_workspaces = []
        for resp in responses:
            if 'error' in resp:
                errs.append(resp['error'])
            else:
                ws_workspaces.append(resp['result'])
        try:
            re_client.save(vert_name, ws_workspaces)
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append({'message': str(err)})
    return {'errors': errs}


def import_workspace_perms(params):
    """
    Import user-to-workspace permission data (using wsfull_user and wsfull_ws_perm edges).
    Pass in the start and stop for the workspace id range (eg. 1 to 100000).
    """
    vert_name = 'wsfull_user'
    edge_name = 'wsfull_ws_perm'
    errs = []
    # Import in batches of 1000
    for responses in split(generate_workspace_perms(params['start'], params['stop']), 1000):
        docs = []  # type: list
        for resp in responses:
            if 'error' in resp:
                errs.append(resp['error'])
            else:
                docs += resp['result']
        user_docs = [d['doc'] for d in docs if d['coll'] == vert_name]
        edge_docs = [d['doc'] for d in docs if d['coll'] == edge_name]
        # Import the user docs
        try:
            re_client.save(vert_name, user_docs)
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append({'message': str(err)})
        # Import the permission edges
        try:
            resp = re_client.save(edge_name, edge_docs)
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append({'message': str(err)})
        return {'errors': errs}


def import_objects(params):
    """
    For a given set of user IDs and/or workspace IDs
    - iterate over all the workspaces
    - iterate over all the objects in each workspace
    - save object and copy/provenance/reference vertices and edges
        (if any edge or vertex exists already, we update it)
    """
    errs = []  # type: list
    ws_infos = []  # type: list
    save_count = 0
    print('workspace_ids', params['workspace_ids'])
    if 'user_ids' in params:
        for (info, err) in _fetch_workspaces_from_users(params['user_ids']):
            if err:
                errs.append(str(err)[0:200])
            ws_infos.append(info)
    if 'workspace_ids' in params:
        for (info, err) in _fetch_workspaces(params['workspace_ids']):
            if err:
                errs.append(str(err)[0:200])
            ws_infos.append(info)
    import_queues = {}  # type: dict
    for ws_info in ws_infos:
        for (result, err) in generate_objects_and_edges(ws_info):
            if err:
                errs.append(str(err)[0:200])
                continue
            coll_name = result['coll']
            doc = result['doc']
            queue_for_import(coll_name, doc, import_queues)
    import_remaining_queues(import_queues)
    return {'errors': errs, 'save_count': save_count}


def queue_for_import(coll_name, doc, queues):
    _queue_max_len = 1000
    if coll_name not in queues:
        queues[coll_name] = []
    queues[coll_name].append(doc)
    if len(queues[coll_name]) > _queue_max_len:
        re_client.save(coll_name, queues[coll_name])
        queues[coll_name] = []


def import_remaining_queues(queues):
    for (coll_name, docs) in queues.items():
        re_client.save(coll_name, docs)


def show_config(params):
    """Show public configuration settings."""
    config = get_config()
    return {
        'token_set?': bool(config['token']),
        'relation_engine_url': config['relation_engine_url'],
        'workspace_url': config['ws_url']
    }


def _fetch_workspaces(ws_ids):
    """Fetch a workspaces by IDs."""
    for ws_id in ws_ids:
        print('Workspace', ws_id)
        try:
            info = workspace_client.admin_req('getWorkspaceInfo', {'id': ws_id})
            # info = workspace_client.req('get_workspace_info', {'id': ws_id})
            yield (info, None)
        except Exception as err:
            yield (None, err)


def _fetch_workspaces_from_users(uids):
    """Fetch workspace info from user IDs/usernames."""
    for uid in uids:
        try:
            info = workspace_client.admin_req('listWorkspaces', {'owners': [uid]})
            # info = workspace_client.req('list_workspace_info', {'owners': [uid]})
            return (info, None)
        except Exception as err:
            return (None, err)
