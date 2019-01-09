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
from clients import workspace_client
from clients import re_client

_obj_vert_name = 'wsprov_object'
_copy_edge_name = 'wsprov_copied_into'
_link_edge_name = 'wsprov_links'
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
    for ws in workspaces:
        for objects in _split(_fetch_objects(ws), 1000):
            for obj in objects:
                docs = _create_db_docs(ws, obj)
                print('Importing %s objects' % len(docs[_obj_vert_name]))
                results = re_client.save(_obj_vert_name, docs[_obj_vert_name])
                print('Results:', results)
                print('Importing %s copies' % len(docs[_copy_edge_name]))
                results = re_client.save(_copy_edge_name, docs[_copy_edge_name])
                print('Results:', results)
                print('Importing %s %s' % (len(docs[_link_edge_name]), _link_edge_name))
                results = re_client.save(_link_edge_name, docs[_link_edge_name])
                print('Results:', results)
    return {'status': 'done'}


def _create_db_docs(ws, obj):
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
    obj_info = obj['info']
    obj_db_key = _get_upa_from_obj_info(obj_info, delimiter=_upa_delimiter)
    obj_db_id = _obj_vert_name + '/' + obj_db_key  # eg. "wsprov_object/1:2:3
    result[_obj_vert_name].append({
        '_key': obj_db_key,
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
    result[_link_edge_name] = []
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
            '_from': obj_db_key,
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


def _fetch_objects(ws):
    """
    Given workspace info, fetch all the objects in that workspace.
    (Metadata only, no content data, such as features)
    """
    ws_id = ws[0]
    obj_infos = workspace_client.admin_req('listObjects', {'ids': [ws_id]})
    obj_upas = [_get_upa_from_obj_info(o) for o in obj_infos]
    get_obj_params = [{'ref': upa} for upa in obj_upas]
    if get_obj_params:
        return workspace_client.admin_req('getObjects', {'objects': get_obj_params, 'no_data': '1'})['data']
    else:
        return []


def _fetch_workspaces(ws_ids):
    """Fetch a workspaces by IDs."""
    workspaces = []  # type: list
    for ws_id in ws_ids:
        workspaces.append(workspace_client.admin_req('getWorkspaceInfo', {'ws_id': ws_id}))
    return workspaces


def _fetch_workspaces_from_users(uids):
    """Fetch workspace info from user IDs/usernames."""
    workspaces = []  # type: list
    for uid in uids:
        workspaces.extend(workspace_client.admin_req('listWorkspaces', {'owners': [uid]}))
    return workspaces


def _split(ls, n):
    """Split the list into sub-lists of max length n."""
    for i in range(0, len(ls), n):
        yield ls[i:i + n]
