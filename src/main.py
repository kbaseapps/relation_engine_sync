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
import time
import traceback
from clients import workspace_client
from clients import re_client
from utils.get_config import get_config

from generate_workspace_infos import generate_workspace_infos

_OBJ_VERT_NAME = 'wsprov_object'
_COPY_EDGE_NAME = 'wsprov_copied_into'
_LINK_EDGE_NAME = 'wsprov_links'
_COLL_NAMES = [_OBJ_VERT_NAME, _COPY_EDGE_NAME, _LINK_EDGE_NAME]
# In arango, the upa "1/2/3" is stored as "1:2:3"
_UPA_DELIMITER = ':'

_WSFULL_VERT_NAME = 'wsfull_workspace'


def import_workspaces(params):
    """
    Import workspace infos.
    Uses the 'wsfull' namespace.
    Pass in the start and stop for the workspace id range (eg. 1 to 100000)
    """
    errs = []
    imported_count = 0
    for responses in _split(generate_workspace_infos(params['start'], params['stop']), 1000):
        ws_workspaces = []
        for resp in responses:
            if 'error' in resp:
                errs.append(resp['error'])
            else:
                ws_workspaces.append(resp['result'])
        try:
            re_client.save(_WSFULL_VERT_NAME, ws_workspaces)
            imported_count += 1
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append({'message': err})
    return {'imported': imported_count, 'errors': errs}


def update_provenance(params):
    """
    For a given set of user IDs and/or workspace IDs
    - iterate over all the workspaces
    - iterate over all the objects in each workspace
    - save object and copy/provenance/reference vertices and edges
        (if any edge or vertex exists already, we update it)
    """
    workspaces = []  # type: list
    if 'user_ids' in params:
        workspaces.extend(_fetch_workspaces_from_users(params['user_ids']))
    if 'workspace_ids' in params:
        workspaces.extend(_fetch_workspaces(params['workspace_ids']))
    # print(workspaces)
    # print(f"Importing from {len(workspaces)} workspaces")
    for ws in workspaces:
        print("workspace", ws)
        # For each set of 1000 objects in the workspace
        for objects in _fetch_objects(ws):
            print('objects len', len(objects))
            # For each object in the set of 1000 objects
            docs = _create_db_docs(ws, objects)
            for name in _COLL_NAMES:
                _import_docs(name, docs[name])
                print('Imported {} {}'.format(len(docs[name]), name))
        print('---')
    return {'status': 'done'}


def import_range(params):
    """
    Given a range of workspace IDs, import all objects within them.
    """
    for ws_id in range(params['start'], params['stop']):
        try:
            update_provenance({'workspace_ids': [ws_id]})
        except Exception as err:
            print('Import error on workspace ', ws_id)
            print(str(err))
    return {'status': 'done'}


def show_config(params):
    """Show public configuration settings."""
    config = get_config()
    return {
        'token_set?': bool(config['token']),
        'relation_engine_url': config['relation_engine_url'],
        'workspace_url': config['ws_url']
    }


def _import_docs(name, docs):
    """Import array of `docs` into collection `name` on arango."""
    if not docs:
        return
    re_client.save(name, docs)


def _collect(gen, n_items):
    items = []
    for item in gen:
        items.append(item)
        if len(items) >= n_items:
            yield items


def _create_db_docs(ws, ws_objects):
    """
    Given a workspace object's metadata
      (type: https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.ObjectData)
    And a workspace info tuple
      (type: https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info)
    Create dicts that can be saved to the Arango database
    Returns a dictionary where each key is an arango collection name, and each val is a list of docs to save
    """
    # `result` is the returned value. We will be appending documents into these lists
    result = {_LINK_EDGE_NAME: [], _OBJ_VERT_NAME: [], _COPY_EDGE_NAME: []}  # type: dict
    ws_id = ws[0]  # eg. "12345"
    owner = ws[2]  # eg. "person123"
    metadata = ws[-1]  # last element is additional workspace metadata
    # 'narrative_nice_name' is often set but looks to not be required
    # Fall back to the workspace id
    narr_name = metadata.get('narrative_nice_name', 'Narrative ' + str(ws_id))
    # See here: https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.permission
    is_public = ws[6] == 'r'
    for obj in ws_objects:
        obj_info = obj['info']
        obj_db_key = _get_upa_from_obj_info(obj_info, delimiter=_UPA_DELIMITER)
        obj_db_id = _OBJ_VERT_NAME + '/' + obj_db_key  # eg. "wsprov_object/1:2:3
        result[_OBJ_VERT_NAME].append({
            '_key': obj_db_key,
            'is_public': is_public,
            'deleted': False,  # TODO get this info -- I don't see it in object_info or ObjectData
            'narr_name': narr_name,
            'workspace_id': ws_id,
            'ws_type': obj_info[2],  # eg. "KBaseGenome.ContigSet"
            'save_date': obj_info[3],  # timestamp
            'checksum': obj_info[8],  # md5 hash
            'owner': owner,  # username
            'obj_name': obj_info[1]  # arbitrary string
        })
        # Check if this object was copied (if so, create a copy edge)
        if 'copied' in obj and not obj.get('copy_source_inaccessible'):
            copy_db_key = obj['copied'].replace('/', _UPA_DELIMITER)  # eg. "1:2:3"
            from_obj_id = _OBJ_VERT_NAME + '/' + copy_db_key  # eg. "wsprov_object/1:2:3"
            copy_doc = {'_from': from_obj_id, '_to': obj_db_id, 'workspace_id': ws_id}
            result[_COPY_EDGE_NAME].append(copy_doc)
        # Generate provenance edges
        provenance = obj['provenance']
        # Create object link edges for every provenance action
        for prov in provenance:
            for input_upa in prov.get('resolved_ws_objects', []):
                input_key = input_upa.replace('/', _UPA_DELIMITER)
                link_doc = {
                    '_from': _OBJ_VERT_NAME + '/' + input_key,
                    '_to': obj_db_id,
                    'type': 'provenance',
                    'service': prov.get('service'),
                    'service_ver': prov.get('service_ver'),
                    'epoch': prov.get('epoch'),
                    'ws_id': ws_id,
                    'method': prov.get('method')
                }
                result[_LINK_EDGE_NAME].append(link_doc)
        # For each reference in this object, create an object link edge
        for ref_upa in obj.get('refs', []):
            ref_key = ref_upa.replace('/', _UPA_DELIMITER)
            link_doc = {
                '_from': obj_db_id,
                '_to': _OBJ_VERT_NAME + '/' + ref_key,
                'ws_id': ws_id,
                'type': 'reference'
            }
            result[_LINK_EDGE_NAME].append(link_doc)
    return result


def _get_upa_from_obj_info(obj_info, delimiter='/'):
    """
    Get the upa, such as "1/2/3", from an obj_info tuple.
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    """
    return delimiter.join([str(obj_info[6]), str(obj_info[0]), str(obj_info[4])])


def _list_objects(ws, min_obj_id=1):
    """
    Generate object info tuples for a given workspace
    Warning: recursive
    """
    print('Starting with object', min_obj_id)
    ws_id = ws[0]
    # obj_infos = workspace_client.admin_req('listObjects', {'ids': [ws_id], 'minObjectID': min_obj_id})
    obj_infos = workspace_client.req('list_objects', {'ids': [ws_id], 'minObjectID': min_obj_id})
    print('Result length:', len(obj_infos))
    for info in obj_infos:
        yield info
    # Workspace API returns a max of 10k results. We can fetch the next page using the obj id
    if len(obj_infos) >= 10000:
        next_obj_id = obj_infos[-1][0] + 1
        for info in _list_objects(ws, min_obj_id=next_obj_id):
            yield info


def _fetch_objects(ws):
    """
    Given workspace info, generate all the object data in that workspace (from 'get_objects2')
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.GetObjects2Params
    (Metadata only, no content data, such as genome features)
    yields 1000 results at a time
    """
    for obj_infos in _split(_list_objects(ws), 1000):
        obj_upas = [_get_upa_from_obj_info(info) for info in obj_infos]
        get_obj_params = [{'ref': upa} for upa in obj_upas]
        start_time = time.time()
        if len(obj_infos):
            print('Calling getObjects on {} upas'.format(len(obj_infos)))
            # yield workspace_client.admin_req('getObjects', {'objects': get_obj_params, 'no_data': '1'})['data']
            yield workspace_client.req('get_objects2', {'objects': get_obj_params, 'no_data': '1'})['data']
            print('Total time was {}'.format(time.time() - start_time))
        else:
            yield []


def _fetch_workspaces(ws_ids):
    """Fetch a workspaces by IDs."""
    workspaces = []  # type: list
    for ws_id in ws_ids:
        print('Workspace', ws_id)
        # workspaces.append(workspace_client.admin_req('getWorkspaceInfo', {'id': ws_id}))
        workspaces.append(workspace_client.req('get_workspace_info', {'id': ws_id}))
    return workspaces


def _fetch_workspaces_from_users(uids):
    """Fetch workspace info from user IDs/usernames."""
    workspaces = []  # type: list
    for uid in uids:
        # workspaces.extend(workspace_client.admin_req('listWorkspaces', {'owners': [uid]}))
        workspaces.extend(workspace_client.req('list_workspace_info', {'owners': [uid]}))
    return workspaces


def _split(ls, n):
    """Split an iterable into sub-lists of max length n."""
    sublist = []  # type: list
    for item in ls:
        if len(sublist) >= n:
            yield sublist
            sublist = []
        sublist.append(item)
    yield sublist
