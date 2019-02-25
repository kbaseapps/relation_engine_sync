

def split(ls, n):
    """Split an iterable into sub-lists of max length n."""
    sublist = []  # type: list
    for item in ls:
        if len(sublist) >= n:
            yield sublist
            sublist = []
        sublist.append(item)
    yield sublist
