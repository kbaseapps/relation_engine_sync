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
_upa_delimiter = ':'

# TODO fetch human-readable narrative names for every object
# Not sure how to do this -- need it for the UI


def update_provenance(params):
    """
    For a given user ID:
    - iterate over all their workspaces
    - iterate over all their objects
    - save object and action vertices and edges
        (if any edge or vertex exists already, we update it)
    """
    uid = params['uid']
    workspaces = workspace_client.req('list_workspace_info', {'owners': [uid]})
    # Delimiters for workspace refs (can't use '/' in arango) (eg. "1:2:3")
    _upa_delimiter = ':'
    # Objects and edges to save in the database
    db_objects = []  # type: ignore
    db_copies = []  # type: ignore
    db_links = []  # type: ignore
    # The result will be a list of:
    #   https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info
    for ws in workspaces:
        ws_id = ws[0]  # workspace id
        owner = ws[2]  # username
        metadata = ws[-1]
        narr_name = metadata.get('narrative_nice_name', 'Narrative ' + str(ws_id))
        # fetch a list of https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
        print('Running Workspace.list_objects...')
        obj_infos = workspace_client.req('list_objects', {'ids': [ws_id]})
        for obj_info in obj_infos:
            ws_type = obj_info[2]  # type of the object
            if 'KBaseNarrative.Narrative' in ws_type:
                # Ignore Narrative objects
                continue
            # Workspace address of this object - "1:2:3"
            obj_addr = _upa_delimiter.join([str(ws_id), str(obj_info[0]), str(obj_info[4])])
            db_objects.append({
                '_key': obj_addr,
                'deleted': False,
                'narr_name': narr_name,
                'workspace_id': ws_id,
                'ws_type': obj_info[2],
                'save_date': obj_info[3],
                'checksum': obj_info[8],
                'owner': owner,
                'obj_name': obj_info[1]
            })
    if not len(db_objects):
        print('No results for ' + uid)
        return {}
    # Parameters for calling Workspace.get_objects2
    obj_upas = [o['_key'].replace(_upa_delimiter, '/') for o in db_objects]
    get_obj_params = [{'ref': upa} for upa in obj_upas]
    print('Running Workspace.get_objects2...')
    ws_objects = workspace_client.req('get_objects2', {
        'objects': get_obj_params,
        'no_data': '1'
    })['data']
    # Create the provenance actions and edges for every object
    for (ws_obj, db_obj) in zip(ws_objects, db_objects):
        provenance = ws_obj['provenance']
        ws_id = ws_obj['info'][6]  # workspace reference/id
        obj_id = _obj_vert_name + '/' + db_obj['_key']  # object id for arango (eg "wsprov_object/1:2:3")
        # TODO list referencing objects and references (add links for each)
        # Check if this object was copied (if so, create a copy action)
        if 'copied' in ws_obj and not ws_obj.get('copy_source_inaccessible'):
            # the key will look like "copy-1:2:3-9:8:7" (ie "copy-from-to")
            # Create action vertex with some attributes for the copy data
            copy_ref = ws_obj['copied'].replace('/', _upa_delimiter)
            from_obj_id = _obj_vert_name + '/' + copy_ref
            copy_doc = {
                '_from': from_obj_id,
                '_to': obj_id,
                'workspace_id': ws_id
            }
            db_copies.append(copy_doc)
        # Create action, produced, and input_in documents for any provenance actions
        for prov in provenance:
            for input_upa in prov.get('resolved_ws_objects', []):
                input_key = input_upa.replace('/', _upa_delimiter)
                link_doc = {
                    '_from': _obj_vert_name + '/' + input_key,
                    '_to': obj_id,
                    'type': 'provenance',
                    'service': prov.get('service'),
                    'service_ver': prov.get('service_ver'),
                    'epoch': prov.get('epoch'),
                    'ws_id': ws_id,
                    'method': prov.get('method')
                }
                db_links.append(link_doc)
        for ref_upa in ws_obj.get('refs', []):
            ref_key = ref_upa.replace('/', _upa_delimiter)
            link_doc = {
                '_from': obj_id,
                '_to': _obj_vert_name + '/' + ref_key,
                'ws_id': ws_id,
                'type': 'reference'
            }
            db_links.append(link_doc)
    # Returns a list of lists of object info (obj_id, obj_name, type_string, ...)
    ref_obj_infos = workspace_client.req('list_referencing_objects', get_obj_params)
    for (obj_info_list, upa) in zip(ref_obj_infos, obj_upas):
        if not len(obj_info_list):
            continue
        for obj_info in obj_info_list:
            ws_id = str(obj_info[6])
            obj_id = _obj_vert_name + '/' + upa.replace('/', _upa_delimiter)
            referencer_key = _upa_delimiter.join([ws_id, str(obj_info[0]), str(obj_info[4])])
            link_doc = {
                '_from': _obj_vert_name + '/' + referencer_key,
                '_to': obj_id,
                'type': 'reference',
                'ws_id': ws_id
            }
    # For each object, call Workspace.list_referencing_objects and Workspace.
    # Make calls to the relation engine API to save/update the documents
    print('Saving all objects to ArangoDB...')
    re_client.save(_obj_vert_name, db_objects)
    re_client.save(_copy_edge_name, db_copies)
    re_client.save(_link_edge_name, db_links)
    return {}
