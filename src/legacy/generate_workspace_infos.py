import time
from src.clients import workspace_client


def generate_workspace_infos(start, stop):
    """
    Generate wsfull_workspace documents for each workspace.
    Args:
        start - workspace id to start importing on
        stop - workspace id to stop before
    yields a dict with either a 'result' or 'error' key.
        'result' will be the wsfull_workspace document data
        'error' will have 'message' and 'wsid'
    """
    for wsid in range(start, stop):
        try:
            ws_info = workspace_client.admin_req('getWorkspaceInfo', {'id': wsid})
        except Exception as err:
            print(err)
            yield {'error': {'message': str(err), 'wsid': wsid}}
            continue
        metadata = ws_info[-1]  # last element is additional workspace metadata
        ws_workspace = {
            '_key': str(wsid),
            'name': ws_info[1],
            'narr_name': metadata.get('narrative_nice_name', ''),
            'owner': ws_info[2],
            'mod_epoch': _timestamp_to_epoch(ws_info[3]),
            'max_obj_id': ws_info[4],
            'is_public': ws_info[6] == 'r',
            'lock_status': ws_info[7]
        }
        yield {'result': ws_workspace}


def _timestamp_to_epoch(ts):
    """Convert a string timestamp into a ms epoch integer."""
    return int(time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")))
