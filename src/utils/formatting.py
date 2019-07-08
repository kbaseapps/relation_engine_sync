import time


def ts_to_epoch(ts):
    """Convert a string timestamp into a ms epoch integer."""
    return int(time.mktime(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))) * 1000
