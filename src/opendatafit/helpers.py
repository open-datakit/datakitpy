"""Miscellaneous helper functions"""


def find(array, key, value):
    """Equivalent of JS find() helper"""
    for i in array:
        if i[key] == value:
            return i

    return None


def find_by_name(array, name):
    """Given an array of objects with a "name" key, return the first object
    matching the name argument
    """
    return find(array, "name", name)


def dataframe_has_index(df):
    """Return true if DataFrame has an index explicitly set, false if not"""
    # Check if DataFrame index is named as a surrogate for whether an index is
    # explicitly set on the data or not
    # See discussion here: https://stackoverflow.com/a/69498942
    return df.index.names[0]
