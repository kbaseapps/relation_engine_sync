from clients import workspace_client


def generate_workspace_perms(start, stop):
    """
    Generate wsfull_ws_perm documents for each workspace.
    Args:
        start - workspace id to start importing on
        stop - workspace id to stop before
    yields a dict with either a 'result' or 'error' key.
        'result' will be the document data for import
            'result'.'user' is the wsfull_user doc
            'result'.'
        'error' will have 'message' and 'wsid'
    """
    user_vert_name = 'wsfull_user'
    ws_vert_name = 'wsfull_workspace'
    edge_name = 'wsfull_ws_perm'
    for wsid in range(start, stop):
        try:
            perms = workspace_client.admin_req('getPermissionsMass', {'workspaces': [{'id': wsid}]})
        except Exception as err:
            print(err)
            yield {'error': {'message': str(err)}}
            continue
        for (username, perm_code) in perms['perms'][0].items():
            if username == '*':
                continue
            edge_doc = {
                '_from': user_vert_name + '/' + username,
                '_to': ws_vert_name + '/' + str(wsid),
                'perm': perm_code
            }
            user_doc = {'_key': username}
            docs = [{'doc': user_doc, 'coll': user_vert_name},
                    {'doc': edge_doc, 'coll': edge_name}]
            yield {'result': docs}