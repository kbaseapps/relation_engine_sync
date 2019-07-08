import time


def ts_to_epoch(ts):
    """Convert a string timestamp into a ms epoch integer."""
    return int(time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))) * 1000


def get_method_key_from_prov(prov):
    """
    From provenance data, such as:
     {
          "service": "narrative",
          "service_ver": "3.10.0",
          "input_ws_objects": [],
          "resolved_ws_objects": [],
          "external_data": [],
          "subactions": [],
          "custom": {},
          "description": "Saved by KBase Narrative Interface"
      }
    Return a string for the wsfull_method_version _key field
    in the format:  "service_name:commit_hash:method_name"
    """
    serv = prov[0]['service']
    commit = None
    subacts = prov[0].get('subactions')
    # Fetch the commit hash from the first subaction, fall back to the semantic version, or finally UNKONWN
    if subacts:
        commit = subacts[0].get('commit')
    if not commit:
        commit = prov[0].get('service_ver', 'UNKNOWN')
    meth = prov[0].get('method', 'UNKNOWN')
    return f"{serv}:{commit}:{meth}"
