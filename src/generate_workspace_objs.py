"""
Generate workspace objects along with provenance, copy, and reference edges.
"""
from clients import workspace_client
from utils.timestamp_to_epoch import timestamp_to_epoch

_UPA_DELIMITER = ':'
_METHOD_VERT_NAME = 'wsfull_method_version'
_METHOD_EDGE_NAME = 'wsfull_obj_created_with_method'
_OBJ_PROV_EDGE_NAME = 'wsfull_prov_descendant_of'
_OBJ_VERT_NAME = 'wsfull_object_version'
_COPY_EDGE_NAME = 'wsfull_copied_from'
_REF_EDGE_NAME = 'wsfull_refers_to'
_LINK_EDGE_NAME = 'wsprov_links'
_COLL_NAMES = [_OBJ_VERT_NAME, _COPY_EDGE_NAME, _LINK_EDGE_NAME]


def generate_workspace_objs(ws_info):
    """
    Generate wsfull_object documents for each workspace, plus related edges
    (refs, copies, provenance).

    Workspace object metadata type:
      https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.ObjectData
    Workspace info tuple:
      https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info
    Args:
        ws_info - workspace_info tuple
    yields a pair of (result, error), one of which will be None
      `result` will be a pair of (collection_name, docs)
        where `collection_name` is the string name of the collection
        and `docs` is a list of dictionaries of data to save
    """
    # Fetch all non-deleted objects for the workspace
    for (obj_infos, err) in _list_objects(ws_info, show_deleted=False):
        print('NONDELETED', len(obj_infos))
        print('')
        if err:
            yield (None, err)
            continue
        # Fetch object details for each obj_info
        (obj_details, err) = _get_object_details(ws_info, obj_infos)
        if err:
            yield (None, err)
            continue
        # Generate/yield every arango document/edge
        for (coll, doc) in _create_obj_docs(ws_info, obj_details, deleted=False):
            yield ((coll, doc), None)
    # Fetch deleted objects in each workspace
    # This is a more limited import. We just import one wsfull_object_version
    # per deleted object from the objec_info tuple. We cannot fetch deleted
    # objects using get_objects2.
    for (obj_infos, err) in _list_objects(ws_info, show_deleted=True):
        if err:
            yield (None, err)
            continue
        for obj_info in obj_infos:
            workspace_client.admin_req
            # Create a partial wsfull_object_version from the object_info alone.
            doc = _create_obj_doc(ws_info, obj_info, deleted=True)
            yield ((_OBJ_VERT_NAME, doc), None)


def _create_obj_doc(ws_info, obj_info, deleted):
    """Create an arango document for some object info."""
    return {
        '_key': _get_upa_from_obj_info(obj_info, delimiter=_UPA_DELIMITER),
        'workspace_id': obj_info[6],
        'object_id': obj_info[0],
        'version': obj_info[4],
        'name': obj_info[1],
        'hash': obj_info[8],
        'size': obj_info[9],
        'epoch': timestamp_to_epoch(obj_info[3]),
        'deleted': deleted,
        'is_public': ws_info[6] == 'r',
        'ws_type': obj_info[2],
        'owner': ws_info[2]
    }


def _create_obj_docs(ws_info, obj_details, deleted):
    """
    Yield object and object edge docs for every set of object data.
    Should throw and return no errors.
    yields pairs of (collection_name, doc)
    """
    # metadata = ws_info[-1]  # last element is additional workspace metadata
    # 'narrative_nice_name' is often set but looks to not be required
    # narr_name = metadata.get('narrative_nice_name', '')
    # See here: https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.permission
    for obj in obj_details:
        obj_info = obj['info']
        doc = _create_obj_doc(ws_info, obj_info, deleted=False)
        yield (_OBJ_VERT_NAME, doc)
        ws_id = ws_info[0]
        obj_db_key = _get_upa_from_obj_info(obj_info, delimiter=_UPA_DELIMITER)
        obj_db_id = _OBJ_VERT_NAME + '/' + obj_db_key  # eg. "wsprov_object/1:2:3
        # Create copy edge, if present, using wsfull_copied_from
        if 'copied' in obj and not obj.get('copy_source_inaccessible'):
            copy_db_key = obj['copied'].replace('/', _UPA_DELIMITER)  # eg. "1:2:3"
            to_obj_id = _OBJ_VERT_NAME + '/' + copy_db_key  # eg. "wsprov_object/1:2:3"
            copy_doc = {'_from': obj_db_id, '_to': to_obj_id, 'workspace_id': ws_id}
            yield (_COPY_EDGE_NAME, copy_doc)
        # For each reference, create edges (wsfull_refers_to)
        for ref_upa in obj.get('refs', []):
            ref_key = ref_upa.replace('/', _UPA_DELIMITER)
            ref_doc = {
                '_from': obj_db_id,
                '_to': _OBJ_VERT_NAME + '/' + ref_key,
                'ws_id': ws_id
            }
            yield (_REF_EDGE_NAME, ref_doc)
        # Create provenance edges for the object
        #  - Method to object with wsfull_object_created_with_method
        #  - Object to object with wsfull_prov_descendant_of
        #  - Object to method with wsfull_object_input_in
        # Generate provenance edges
        provenance = obj['provenance']
        # Create object link edges for every provenance action
        for prov in provenance:
            # Create a module/method/version vertex
            module_name = prov.get('service', 'UNKNOWN')
            subactions = prov.get('subactions', [])
            subact = subactions[0] if subactions else {}
            commit = subact.get('commit', 'UNKNOWN')
            code_url = subact.get('code_url', 'UNKOWN')
            module_ver = prov.get('service_ver', 'UNKNOWN')
            method_name = prov.get('method', 'UNKNOWN')
            method_key = module_name + ':' + (commit or module_ver) + '.' + method_name
            method_doc = {
                    '_key': method_key,
                    'module_name': module_name,
                    'method_name': method_name,
                    'commit': commit,
                    'code_url': code_url,
                    'module_ver': module_ver
            }
            yield (_METHOD_VERT_NAME, method_doc)
            # Create an edge from this object to the method (wsfull_object_output_from)
            method_edge_doc = {
                '_from': obj_db_id,
                '_to': _METHOD_VERT_NAME + '/' + method_doc['_key'],
                'method_params': prov.get('method_params')
            }
            yield (_METHOD_EDGE_NAME, method_edge_doc)
            # Create an edge from this object to any input objects (wsfull_prov_descendant_of)
            for input_upa in prov.get('resolved_ws_objects', []):
                input_key = input_upa.replace('/', _UPA_DELIMITER)
                prov_edge_doc = {
                    '_from': obj_db_id,
                    '_to': _OBJ_VERT_NAME + '/' + input_key,
                    _METHOD_VERT_NAME: method_doc['_key']
                }
                yield (_OBJ_PROV_EDGE_NAME, prov_edge_doc)


def _get_object_details(ws_info, obj_infos):
    """
    Given workspace info, generate all the object data in that workspace (from 'get_objects2')
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.GetObjects2Params
    yields pair of (result, err), one of which will be None
        result will be a list of up to 10k object detail objects from get_objects2
    """
    if not obj_infos:  # empty list
        return
    obj_upas = [_get_upa_from_obj_info(info) for info in obj_infos]
    get_obj_params = [{'ref': upa} for upa in obj_upas]
    try:
        obj_details = workspace_client.admin_req('getObjects', {
            'objects': get_obj_params,
            'no_data': '1'
        })['data']
        return (obj_details, None)
    except Exception as err:
        return (None, err)


def _get_upa_from_obj_info(obj_info, delimiter='/'):
    """
    Get the upa, such as "1/2/3", from an obj_info tuple.
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    """
    return delimiter.join([str(obj_info[6]), str(obj_info[0]), str(obj_info[4])])


def _list_objects(ws_info, show_deleted, min_obj_id=1):
    """
    Generate object info tuples for a given workspace_info tuple
    For workspaces with more than 10k objects, we have to do some page iteration
    Warning: recursive
    yields pair of (result, err), one of which will be None
        result is the object_info tuple for each object
    """
    ws_id = ws_info[0]
    try:
        obj_infos = workspace_client.admin_req('listObjects', {
            'ids': [ws_id],
            'showDeleted': int(show_deleted),
            'showHidden': 1,
            'showAllVersions': 1,
            'minObjectID': min_obj_id
        })
        yield (obj_infos, None)
    except Exception as err:
        yield (None, err)
        return
    # Workspace API returns a max of 10k results. We can fetch the next page using the obj id
    if len(obj_infos) >= 10000:
        next_obj_id = obj_infos[-1][0] + 1
        for info in _list_objects(ws_info, show_deleted, min_obj_id=next_obj_id):
            yield info
