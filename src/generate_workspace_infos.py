from clients import workspace_client
from utils.timestamp_to_epoch import timestamp_to_epoch


def generate_workspace_infos(start, stop):
    """
    Generate wsfull_workspace documents for each workspace.
    Args:
        start - workspace id to start importing on
        stop - workspace id to stop before
    yields a pair of (result, error), one of which will be None
    """
    for wsid in range(start, stop):
        try:
            ws_info = workspace_client.admin_req('getWorkspaceInfo', {'id': wsid})
        except Exception as err:
            print(err)
            yield (None, {'message': str(err), 'wsid': wsid})
            continue
        metadata = ws_info[-1]  # last element is additional workspace metadata
        ws_workspace = {
            '_key': str(wsid),
            'name': ws_info[1],
            'narr_name': metadata.get('narrative_nice_name', ''),
            'owner': ws_info[2],
            'mod_epoch': timestamp_to_epoch(ws_info[3]),
            'max_obj_id': ws_info[4],
            'is_public': ws_info[6] == 'r',
            'lock_status': ws_info[7]
        }
        yield (ws_workspace, None)
