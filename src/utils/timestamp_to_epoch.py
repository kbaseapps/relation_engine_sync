import time


def timestamp_to_epoch(ts):
    """Convert a string timestamp into a ms epoch integer."""
    return int(time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z")))
