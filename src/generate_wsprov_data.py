import json

from src.clients import workspace_client

_OBJ_VERT_NAME = 'wsprov_object'
_COPY_EDGE_NAME = 'wsprov_copied_into'
_LINK_EDGE_NAME = 'wsprov_links'
_UPA_DELIMITER = ':'


def generate_wsprov_data(wsid, min_obj_id, max_obj_id, files):
    """
    Iterate over the objects in a workspace and write JSON documents to a set
    of files that represent arango vertices and edges for each object.
    Args:
        wsid - workspace id integer
        min_obj_id - minimum object id to start importing (integer)
        files - dictionary where keys are collection names and values are opened file descriptors
            files should be oppened in APPEND mode
    """
    # Get the workspace info
    ws_info = workspace_client.admin_req('getWorkspaceInfo', {'id': wsid})
    print('Fetched workspace info:', ws_info)
    is_public = ws_info[6] == 'r'
    metadata = ws_info[-1]
    narr_name = metadata.get('narrative_nice_name')
    owner = ws_info[2]
    # Fetch all the objects for the workspace
    obj_infos = workspace_client.admin_req('listObjects', {
        'ids': [wsid],
        'showAllVersions': 1,
        'minObjectID': min_obj_id,
        'maxObjectID': max_obj_id
    })
    print(f'Fetched {len(obj_infos)} object infos.')
    obj_upas = [_get_upa_from_obj_info(info) for info in obj_infos[0:1000]]
    get_obj_params = [{'ref': upa} for upa in obj_upas]
    print(f'Fetching with get_objects2 on {len(get_obj_params)} objects.')
    obj_data = workspace_client.req('get_objects2', {'objects': get_obj_params, 'no_data': '1'})['data']
    print(f'Fetched {len(obj_data)} object data.')
    for obj in obj_data:
        obj_info = obj['info']
        obj_db_key = _get_upa_from_obj_info(obj_info, delimiter=_UPA_DELIMITER)
        obj_db_id = _OBJ_VERT_NAME + '/' + obj_db_key  # eg. "wsprov_object/1:2:3
        # Write the wsprov_object
        files[_OBJ_VERT_NAME].write(json.dumps({
            '_key': obj_db_key,
            'is_public': is_public,
            'deleted': False,  # TODO get this info -- I don't see it in object_info or ObjectData
            'narr_name': narr_name,
            'workspace_id': wsid,
            'ws_type': obj_info[2],  # eg. "KBaseGenome.ContigSet"
            'save_date': obj_info[3],  # timestamp
            'checksum': obj_info[8],  # md5 hash
            'owner': owner,  # username
            'obj_name': obj_info[1]  # arbitrary string
        }) + '\n')
        # Check if this object was copied (if so, create a copy edge)
        if 'copied' in obj and not obj.get('copy_source_inaccessible'):
            copy_db_key = obj['copied'].replace('/', _UPA_DELIMITER)  # eg. "1:2:3"
            from_obj_id = _OBJ_VERT_NAME + '/' + copy_db_key  # eg. "wsprov_object/1:2:3"
            copy_doc = {'_from': from_obj_id, '_to': obj_db_id, 'workspace_id': wsid}
            files[_COPY_EDGE_NAME].write(json.dumps(copy_doc) + '\n')
        # Create edges for every provenance action
        for prov in obj['provenance']:
            for input_upa in prov.get('resolved_ws_objects', []):
                input_key = input_upa.replace('/', _UPA_DELIMITER)
                link_doc = {
                    '_from': _OBJ_VERT_NAME + '/' + input_key,
                    '_to': obj_db_id,
                    'type': 'provenance',
                    'service': prov.get('service'),
                    'service_ver': prov.get('service_ver'),
                    'epoch': prov.get('epoch'),
                    'ws_id': wsid,
                    'method': prov.get('method')
                }
                files[_LINK_EDGE_NAME].write(json.dumps(link_doc) + '\n')
        # For each reference in this object, create an object link edge
        for ref_upa in obj.get('refs', []):
            ref_key = ref_upa.replace('/', _UPA_DELIMITER)
            link_doc = {
                '_from': obj_db_id,
                '_to': _OBJ_VERT_NAME + '/' + ref_key,
                'ws_id': wsid,
                'type': 'reference'
            }
            files[_LINK_EDGE_NAME].write(json.dumps(link_doc) + '\n')


def _get_upa_from_obj_info(obj_info, delimiter='/'):
    """
    Get the upa, such as "1/2/3", from an obj_info tuple.
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    """
    return delimiter.join([str(obj_info[6]), str(obj_info[0]), str(obj_info[4])])
