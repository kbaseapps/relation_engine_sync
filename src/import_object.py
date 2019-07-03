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
    print("obj_info", obj_info)
    info_tup = obj_info['info']
    wsid = info_tup[6]
    objid = info_tup[0]
    print('xyz saving..')
    save('wsfull_object', [{
        '_key': f'{wsid}:{objid}',
        'workspace_id': wsid,
        'object_id': objid,
        'deleted': False
    }])
    # TODO fetch the doc on RE, blocking for it with timeout
    print('xyz saved.')
