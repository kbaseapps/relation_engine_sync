"""

wsfull_object (unversioned)
    workspace_id
    object_id
    deleted
    _key: wsid/objid
"""

from .utils.re_client import save


def import_object(obj_info):
    """
    Given a workspace object downloaded to disk, convert it to a wsfull arangodb document and import it.
    """
    info_tup = obj_info['info']
    wsid = info_tup[6]
    objid = info_tup[0]
    key = f'{wsid}:{objid}'
    print('Saving wsfull_object with key', key)
    save('wsfull_object', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'deleted': False
    }])
    print('Successfully saved', key)
