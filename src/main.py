"""
This file contains all the methods for this KBase SDK module.

Update graph data in the Relation Engine based on updates on KBase.
"""
import os
import traceback
import tempfile
import shutil
from collections import defaultdict

from src.clients import workspace_client
from src.clients import re_client
from src.utils.get_config import get_config
from src.generate_workspace_infos import generate_workspace_infos
from src.generate_workspace_perms import generate_workspace_perms
from src.generate_workspace_objs import generate_workspace_objs
from src.generate_wsprov_data import generate_wsprov_data

# In arango, the upa "1/2/3" is stored as "1:2:3"


def import_wsprov(params):
    """
    Import into the wsprov* namespace given a workspace ID
    """
    tmp_dir = tempfile.mkdtemp()
    files = {
        'wsprov_object': open(os.path.join(tmp_dir, 'wsprov_object.json'), 'a'),
        'wsprov_copied_into': open(os.path.join(tmp_dir, 'wsprov_copied_into.json'), 'a'),
        'wsprov_links': open(os.path.join(tmp_dir, 'wsprov_links.json'), 'a')
    }
    wsid = params['workspace_id']
    min_obj_id = params.get('min_object_id', 1)
    max_obj_id = params.get('max_object_id', min_obj_id + 1000)
    print(f'Importing WS {wsid} starting with object {min_obj_id}')
    try:
        generate_wsprov_data(wsid, min_obj_id, max_obj_id, files)
        for key, fd in files.items():
            print(f'Importing records from {fd.name}')
            with open(fd.name, 'rb') as fdr:
                re_client.import_file(fd.name, fdr)
            fd.close()
    finally:
        shutil.rmtree(tmp_dir)
    return {}


def import_workspaces(params):
    """
    Import workspace infos into wsfull_workspace.
    Uses the 'wsfull' namespace.
    Pass in the start and stop for the workspace id range (eg. 1 to 100000)
    """
    vert_name = 'wsfull_workspace'
    errs = []
    import_count = 0
    # Import workspaces in batches of 1000
    for responses in _split(generate_workspace_infos(params['start'], params['stop']), 1000):
        # `responses` is a pair of (result, err), one of which will be None
        ws_workspaces = []
        for (result, err) in responses:
            if err:
                errs.append(err)
            else:
                ws_workspaces.append(result)
        try:
            resp = re_client.save(vert_name, ws_workspaces)
            import_count += resp['updated']
            import_count += resp['created']
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append(str(err)[:200])
    return {'errors': errs, 'import_count': import_count}


def import_workspace_perms(params):
    """
    Import user-to-workspace permission data (using wsfull_user and wsfull_ws_perm edges).
    Pass in the start and stop for the workspace id range (eg. 1 to 100000).
    """
    vert_name = 'wsfull_user'
    edge_name = 'wsfull_ws_perm'
    errs = []
    vert_import_count = 0
    edge_import_count = 0
    # Import in batches of 1000
    for responses in _split(generate_workspace_perms(params['start'], params['stop']), 1000):
        # `responses` is a pair of (result, err), one of which will be None
        docs = []  # type: list
        for (result, err) in responses:
            if err:
                errs.append(err)
            else:
                docs += result
        user_docs = [d['doc'] for d in docs if d['coll'] == vert_name]
        edge_docs = [d['doc'] for d in docs if d['coll'] == edge_name]
        # Import the user docs
        try:
            resp = re_client.save(vert_name, user_docs)
            vert_import_count += resp['updated']
            vert_import_count += resp['created']
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append(str(err)[:200])
        # Import the permission edges
        try:
            resp = re_client.save(edge_name, edge_docs)
            edge_import_count += resp['updated']
            edge_import_count += resp['created']
        except Exception as err:
            print(err)
            traceback.print_exc()
            errs.append(str(err)[:200])
        return {'errors': errs, 'vertex_import_count': vert_import_count, 'edge_import_count': edge_import_count}


def import_objects(params):
    """
    Given a list of workspace IDs, import all objects within them, including
    provenance, copy, and reference edges.
    """
    errs = []
    import_data = defaultdict(list)  # type: dict
    imported_count = 0
    for (ws_info, err) in _fetch_workspaces(params['workspace_ids']):
        if err:
            errs.append(str(err)[:200])
            continue
        for (result, err) in generate_workspace_objs(ws_info):
            if err:
                errs.append(str(err)[:200])
                continue
            (coll_name, doc) = result
            print(doc)
            import_data[coll_name].append(doc)
            (count, import_errs) = _import_docs_groups(import_data, 10000)
            errs.extend(import_errs)
            imported_count += count
    (count, import_errs) = _import_docs_groups(import_data, 0)  # Import everything remaining
    errs.extend(import_errs)
    imported_count += count
    return {'errs': errs, 'imported_count': imported_count}


def show_config(params):
    """Show public configuration settings."""
    config = get_config()
    return {
        'token_set?': bool(config['token']),
        'relation_engine_url': config['relation_engine_url'],
        'workspace_url': config['ws_url']
    }


def _import_docs_groups(import_data, min_size):
    """
    Import a set of documents, grouped by collection name.
    Args:
        import_data - dictionary where each key is a collection name and each
          value is a list of documents to save
        min_size - minimum size of each group of documents before we do an
          import. If below the minimum, it is a no-op.
    * Mutates import_data. Sets entries to empty lists that get successfully imported.
    returns a pair of (import_data, errs), where:
        imported_count is the count of everything created/updated/replaced
        errs is a list of error objects
    """
    errs = []
    imported_count = 0
    for (coll_name, docs) in import_data.items():
        if len(docs) < min_size:
            continue
        try:
            print(f'importing {coll_name} with length {len(docs)}')
            resp = re_client.save(coll_name, docs)
            import_data[coll_name] = []
            imported_count += resp['updated']
            imported_count += resp['created']
        except Exception as err:
            errs.append(str(err)[:200])
    return (imported_count, errs)


def _get_upa_from_obj_info(obj_info, delimiter='/'):
    """
    Get the upa, such as "1/2/3", from an obj_info tuple.
    https://kbase.us/services/ws/docs/Workspace.html#typedefWorkspace.object_info
    """
    return delimiter.join([str(obj_info[6]), str(obj_info[0]), str(obj_info[4])])


def _fetch_workspaces(ws_ids):
    """
    Fetch a workspaces by IDs.
    returns a pair of (results, errs) where
        results is a list of workspace infos
        errs is a list of error objects
    """
    for ws_id in ws_ids:
        try:
            ws_info = workspace_client.admin_req('getWorkspaceInfo', {'id': ws_id})
            yield (ws_info, None)
        except Exception as err:
            yield (None, err)


def _split(ls, n):
    """Split an iterable into sub-lists of max length n."""
    sublist = []  # type: list
    for item in ls:
        if len(sublist) >= n:
            yield sublist
            sublist = []
        sublist.append(item)
    yield sublist
