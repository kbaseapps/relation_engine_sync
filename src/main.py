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

_action_vert_name = 'wsprov_action'
_obj_vert_name = 'wsprov_object'
_produced_edge_name = 'wsprov_produced'
_input_edge_name = 'wsprov_input_in'
_similar_edge_name = 'wsprov_similar_to'

# TODO fetch human-readable narrative names for every object
# Not sure how to do this -- need it for the UI


def update_provenance(params):
    """
    For a given user ID:
    - iterate over all their workspaces
    - iterate over all their objects
    - save object and action vertices
        (if any edge or object exists already, we skip it)
    """
    uid = params['uid']
    workspaces = workspace_client.req('list_workspace_info', {'owners': [uid]})
    # Delimiters for workspace refs (can't use '/' in arango) (eg. "1:2:3")
    delimiter = ':'
    # Object and data to save in the database
    db_objects = []  # type: ignore
    db_actions = []  # type: ignore
    db_produced = []  # type: ignore
    db_input_in = []  # type: ignore
    # The result will be a list of:
    #   https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.workspace_info
    for ws in workspaces:
        ws_id = ws[0]  # workspace id
        owner = ws[2]  # username
        # fetch a list of https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
        print('Running Workspace.list_objects...')
        obj_infos = workspace_client.req('list_objects', {'ids': [ws_id]})
        for obj_info in obj_infos:
            ws_type = obj_info[2]  # type of the object
            if 'KBaseNarrative.Narrative' in ws_type:
                # Ignore Narrative objects
                continue
            # Workspace address of this object - "1:2:3"
            obj_addr = delimiter.join([str(ws_id), str(obj_info[0]), str(obj_info[4])])
            db_objects.append({
                '_key': obj_addr,
                'deleted': False,
                'workspace_id': ws_id,
                'ws_type': obj_info[2],
                'save_date': obj_info[3],
                'checksum': obj_info[8],
                'owner': owner,
                'obj_name': obj_info[1]
            })
    # Parameters for calling Workspace.get_objects2
    get_obj_params = [{'ref': o['_key'].replace(delimiter, '/')} for o in db_objects]
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
        # Check if this object was copied (if so, create a copy action)
        if 'copied' in ws_obj and not ws_obj.get('copy_source_inaccessible'):
            copy_ref = ':'.join(ws_obj['copied'].split('/'))
            # the key will look like "copy-1:2:3-9:8:7" (ie "copy-from-to")
            key = 'copy-' + copy_ref + '-' + db_obj['_key']
            # Create action vertex with some attributes for the copy data
            copy_action = {
                '_key': key,
                'workspace_id': ws_id,
                'name': 'copy',
                'copy_from': copy_ref,
                'runner': db_obj['owner'] or '',
                'copy_to': db_obj['_key']
            }
            db_actions.append(copy_action)
            # Create input and produced edges from/to the object
            # Arango id for the action (eg "wsprov_action/key")
            action_id = _action_vert_name + '/' + copy_action['_key']
            # Arango id for the object we copied this one from
            copied_from_id = _obj_vert_name + '/' + copy_ref
            # source_object -> input_in -> action (copy) -> produced -> this_object
            db_produced.append({'_from': action_id, '_to': obj_id})
            db_input_in.append({'_from': copied_from_id, '_to': action_id})
        # Create action, produced, and input_in documents for any provenance actions
        for prov in provenance:
            # The provenance array will always just be 1 element. But let's iterate just in case.
            # An object will only ever be created by 1 action.
            service = prov.get('service', '')
            ver = prov.get('service_ver', '')
            method = prov.get('method', '')
            if service == 'Workspace' and method == '':
                # Skip weird workspace provenance actions from old example data
                # This happens when you add example data in a narrative
                continue
            if not service or not service.strip():
                # We need stuff to have service names at the very least
                continue
            # input_ws_objects = prov['input_ws_objects']
            # Unix timestamp of creation
            ts = prov.get('epoch', -1)
            # key is workspace_id:service_name:method_name:service_version:epoch
            key = delimiter.join([str(ws_id), service, method, ver, str(ts)])
            db_action = {
                '_key': key,
                'name': service + '.' + method,
                'service': service,
                'method': method,
                'version': ver,
                'timestamp': ts,
                'workspace_id': ws_id or '',
                'runner': db_obj['owner'] or ''
            }
            db_actions.append(db_action)
            input_ws_objects = prov['resolved_ws_objects']
            # Create produced edge from action to obj
            db_produced.append({
                '_from': 'wsprov_action/' + db_action['_key'],
                '_to': 'wsprov_object/' + db_obj['_key']
            })
            # Create input_in edges from input_ws_objects to action
            for ref in input_ws_objects:
                # Create an input_in edge from the ref to the action
                db_input_in.append({
                    '_from': 'wsprov_object/' + ref.replace('/', delimiter),
                    '_to': 'wsprov_action/' + db_action['_key']
                })
    # Make calls to the relation engine API to save/update the documents
    print('Saving all objects to ArangoDB...')
    re_client.save(_obj_vert_name, db_objects)
    re_client.save(_action_vert_name, db_actions)
    re_client.save(_produced_edge_name, db_produced)
    re_client.save(_input_edge_name, db_input_in)
    return {}
