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
from clients import workspace_client
from clients import re_client

_obj_vert_name = 'wsprov_object'
_copy_edge_name = 'wsprov_copied_into'
_link_edge_name = 'wsprov_links'
_coll_names = [_obj_vert_name, _copy_edge_name, _link_edge_name]
# In arango, the upa "1/2/3" is stored as "1:2:3"
_upa_delimiter = ':'


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
            for name in _coll_names:
                _import_docs(name, docs[name])
                print('Imported {} {}'.format(len(docs[name]), name))
        print('---')
    return {'status': 'done'}


def import_range(params):
    """
    Given a range of workspace IDs, import all objects within them.
    """
    for ws_id in range(params['start'], params['stop']):
        update_provenance({'workspace_ids': [ws_id]})
    return {'status': 'done'}


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
    result = {_link_edge_name: [], _obj_vert_name: [], _copy_edge_name: []}  # type: dict
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
        obj_db_key = _get_upa_from_obj_info(obj_info, delimiter=_upa_delimiter)
        obj_db_id = _obj_vert_name + '/' + obj_db_key  # eg. "wsprov_object/1:2:3
        result[_obj_vert_name].append({
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
            copy_db_key = obj['copied'].replace('/', _upa_delimiter)  # eg. "1:2:3"
            from_obj_id = _obj_vert_name + '/' + copy_db_key  # eg. "wsprov_object/1:2:3"
            copy_doc = {'_from': from_obj_id, '_to': obj_db_id, 'workspace_id': ws_id}
            result[_copy_edge_name].append(copy_doc)
        # Generate provenance edges
        provenance = obj['provenance']
        # Create object link edges for every provenance action
        for prov in provenance:
            for input_upa in prov.get('resolved_ws_objects', []):
                input_key = input_upa.replace('/', _upa_delimiter)
                link_doc = {
                    '_from': _obj_vert_name + '/' + input_key,
                    '_to': obj_db_id,
                    'type': 'provenance',
                    'service': prov.get('service'),
                    'service_ver': prov.get('service_ver'),
                    'epoch': prov.get('epoch'),
                    'ws_id': ws_id,
                    'method': prov.get('method')
                }
                result[_link_edge_name].append(link_doc)
        # For each reference in this object, create an object link edge
        for ref_upa in obj.get('refs', []):
            ref_key = ref_upa.replace('/', _upa_delimiter)
            link_doc = {
                '_from': obj_db_id,
                '_to': _obj_vert_name + '/' + ref_key,
                'ws_id': ws_id,
                'type': 'reference'
            }
            result[_link_edge_name].append(link_doc)
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
    obj_infos = workspace_client.admin_req('listObjects', {'ids': [ws_id], 'minObjectID': min_obj_id})
    # obj_infos = workspace_client.req('list_objects', {'ids': [ws_id], 'minObjectID': min_obj_id})
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
            yield workspace_client.admin_req('getObjects', {'objects': get_obj_params, 'no_data': '1'})['data']
            # yield workspace_client.req('get_objects2', {'objects': get_obj_params, 'no_data': '1'})['data']
            print('Total time was {}'.format(time.time() - start_time))
        else:
            yield []


def _fetch_workspaces(ws_ids):
    """Fetch a workspaces by IDs."""
    workspaces = []  # type: list
    for ws_id in ws_ids:
        print('Workspace', ws_id)
        workspaces.append(workspace_client.admin_req('getWorkspaceInfo', {'id': ws_id}))
        # workspaces.append(workspace_client.req('get_workspace_info', {'id': ws_id}))
    return workspaces


def _fetch_workspaces_from_users(uids):
    """Fetch workspace info from user IDs/usernames."""
    workspaces = []  # type: list
    for uid in uids:
        workspaces.extend(workspace_client.admin_req('listWorkspaces', {'owners': [uid]}))
        # workspaces.extend(workspace_client.req('list_workspace_info', {'owners': [uid]}))
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
