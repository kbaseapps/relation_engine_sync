"""
wsfull_object (unversioned)
    workspace_id
    object_id
    deleted
    _key: wsid/objid
"""
from src.utils.re_client import save
from src.utils.formatting import ts_to_epoch


def import_object(obj_info):
    """
    Given a workspace object downloaded to disk, convert it to a wsfull arangodb document and import it.
    """
    # Save the wsfull_object document
    info_tup = obj_info['info']
    wsid = info_tup[6]
    objid = info_tup[0]
    obj_key = f'{wsid}:{objid}'
    _save_wsfull_object(obj_key, wsid, objid)
    # Save the wsfull_object_hash document
    _save_obj_hash(info_tup)
    # Save the wsfull_object_version document
    obj_ver = info_tup[4]
    obj_ver_key = f'{obj_key}:{obj_ver}'
    _save_obj_version(obj_ver_key, wsid, objid, obj_ver, info_tup)


def _save_wsfull_object(key, wsid, objid):
    print('Saving wsfull_object with key', key)
    save('wsfull_object', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'deleted': False
    }])
    print('Successfully saved', key)


def _save_obj_hash(info_tup):
    obj_hash = info_tup[8]
    obj_hash_type = 'MD5'
    print('Saving wsfull_object_hash with key', obj_hash)
    save('wsfull_object_hash', [{
        '_key': obj_hash,
        'type': obj_hash_type
    }])
    print('Successfully saved', obj_hash)


def _save_obj_version(key, wsid, objid, ver, info_tup):
    obj_name = info_tup[1]
    hsh = info_tup[8]
    size = info_tup[9]
    epoch = ts_to_epoch(info_tup[3])
    print("Saving wsfull_object version with key", key)
    save('wsfull_object_version', [{
        '_key': key,
        'workspace_id': wsid,
        'object_id': objid,
        'version': ver,
        'name': obj_name,
        'hash': hsh,
        'size': size,
        'epoch': epoch,
        'deleted': False
    }])
    print("Successfully saved", key)
