import time
from utils.split import split
from clients import workspace_client

_UPA_DELIMITER = ':'
_OBJ_VERT_NAME = 'wsfull_object_version'
_COPY_EDGE_NAME = 'wsfull_copied_from'
_PROV_EDGE_NAME = 'wsfull_prov_descendant_of'
_REF_EDGE_NAME = 'wsfull_refers_to'


def generate_objects_and_edges(ws_info):
    """
    Given some workspace info:
    - fetch all the objects (all versions, plus deleted) for the workspace, 10k at a time
    - fetch the full object data (get_objects2), 1000 at a time
    - yield vertices and edges for every object

    Yields pairs of (result, error)

    Workspace info type:
      https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info
    Object info type:
      https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    """
    ws_id = ws_info[0]
    is_public = ws_info[6] == 'r'
    metadata = ws_info[-1]  # last element is additional workspace metadata
    narr_name = metadata.get('narrative_nice_name')
    owner = ws_info[2]  # eg. "person123"
    # `result` is the returned value. We will be appending documents into these lists
    # result = {_LINK_EDGE_NAME: [], _OBJ_VERT_NAME: [], _COPY_EDGE_NAME: []}  # type: dict
    for (objs, err) in _fetch_objects(ws_info):
        if err:
            yield (None, err)
            continue
        for obj in objs:
            obj_id = obj['info'][0]
            version = obj['info'][4]
            obj_db_key = _get_upa_from_obj_info(obj['info'], delimiter=_UPA_DELIMITER)
            obj_db_id = _OBJ_VERT_NAME + '/' + obj_db_key  # eg. "wsprov_object/1:2:3
            doc = {
                '_key': obj_db_key,
                'workspace_id': ws_id,
                'object_id': obj_id,
                'version': version,
                'ws_type': obj['info'][2],
                'name': obj['info'][1],
                'hash': obj['info'][8],  # md5 hash
                'size': obj['info'][9],  # size in bytes
                'public': is_public,  # obj is public if the narrative is public
                'narr_name': narr_name,  # narrative name from the workspace
                'epoch': _timestamp_to_epoch(obj['info'][3]),  # int epoch
                'owner': owner  # username
            }
            yield ({'doc': doc, 'coll': _OBJ_VERT_NAME}, None)
            # Check if this object was copied (if so, create a copy edge)
            if 'copied' in obj and not obj.get('copy_source_inaccessible'):
                copy_db_key = obj['copied'].replace('/', _UPA_DELIMITER)  # eg. "1:2:3"
                from_obj_id = _OBJ_VERT_NAME + '/' + copy_db_key  # eg. "wsfull_object/1:2:3"
                copy_doc = {'_from': from_obj_id, '_to': obj_db_id, 'workspace_id': ws_id}
                yield ({'doc': copy_doc, 'coll': _COPY_EDGE_NAME}, None)
        # Generate provenance edges
        provenance = obj['provenance']
        # Create object link edges for every provenance action
        for prov in provenance:
            for input_upa in prov.get('resolved_ws_objects', []):
                input_key = input_upa.replace('/', _UPA_DELIMITER)
                link_doc = {
                    '_to': _OBJ_VERT_NAME + '/' + input_key,
                    '_from': obj_db_id,
                    'service': prov.get('service'),
                    'service_ver': prov.get('service_ver'),
                    'epoch': prov.get('epoch'),
                    'ws_id': ws_id,
                    'method': prov.get('method')
                }
                yield ({'doc': link_doc, 'coll': _PROV_EDGE_NAME}, None)
        # For each reference in this object, create an object link edge
        for ref_upa in obj.get('refs', []):
            ref_key = ref_upa.replace('/', _UPA_DELIMITER)
            link_doc = {
                '_from': obj_db_id,
                '_to': _OBJ_VERT_NAME + '/' + ref_key
            }
            yield ({'doc': link_doc, 'coll': _REF_EDGE_NAME}, None)


def _list_objects(ws, min_obj_id=1):
    """
    Generate object info tuples for a given workspace
    Warning: recursive
    yields (result, error)
    """
    print('Starting with object', min_obj_id)
    ws_id = ws[0]
    try:
        obj_infos = workspace_client.admin_req('listObjects', {
            'ids': [ws_id],
            'minObjectID': min_obj_id,
            'showHidden': 1,
            # TODO separate query for deleted and non-deleted
            'showDeleted': 1,
            'showAllVersions': 1
        })
    except Exception as err:
        yield (None, err)
        return
    for info in obj_infos:
        yield (info, None)
    # Workspace API returns a max of 10k results. We can fetch the next page using the obj id
    if len(obj_infos) >= 10000:
        next_obj_id = obj_infos[-1][0] + 1
        for info in _list_objects(ws, min_obj_id=next_obj_id):
            yield (info, None)


def _fetch_objects(ws):
    """
    Given workspace info, generate all the object data in that workspace (from 'get_objects2')
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.GetObjects2Params
    (Metadata only, no content data, such as genome features)
    yields (result, error)
    """
    for obj_infos in split(_list_objects(ws), 1000):
        get_obj_params = []  # type: list
        for (info, err) in obj_infos:
            if err:
                yield (None, err)
                continue
            get_obj_params.append({'ref': _get_upa_from_obj_info(info)})
        if not get_obj_params:
            continue
        try:
            objs = workspace_client.admin_req('getObjects', {'objects': get_obj_params, 'no_data': 1})['data']
            yield (objs, None)
        except Exception as err:
            yield (None, err)


def _get_upa_from_obj_info(obj_info, delimiter='/'):
    """
    Get the upa, such as "1/2/3", from an obj_info tuple.
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    """
    return delimiter.join([str(obj_info[6]), str(obj_info[0]), str(obj_info[4])])


def _timestamp_to_epoch(timestamp):
    return int(time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z")))
